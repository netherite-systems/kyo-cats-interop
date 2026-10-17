# kyo-cats

Bidirectional interop between Kyo and Cats Effect. JVM and Scala.js.

```scala
libraryDependencies += "io.github.netherite-systems" %%% "kyo-cats" % "0.1.2"
```

| Direction  | Method                   | Shape                                                                |
| ---------- | ------------------------ | -------------------------------------------------------------------- |
| Cats → Kyo | `CatsToKyo.get`          | `CatsIO[A] => A < (Abort[Nothing] & Async)`                          |
| Cats → Kyo | `CatsToKyo.lift`         | `F[A] => A < (Abort[Nothing] & Async)`, needs `Dispatcher[F]`        |
| Cats → Kyo | `CatsToKyo.liftResource` | `Resource[F, A] => A < (Scope & Abort[Nothing] & Async)`             |
| Kyo → Cats | `KyoToCats.run`          | `A < (Abort[Throwable] & Async) => CatsIO[A]`                        |
| Kyo → Cats | `KyoToCats.runF`         | `A < (Abort[Throwable] & Async) => F[A]`, needs `Async[F]`           |
| Kyo → Cats | `KyoToCats.runResource`  | `A < (Scope & Abort[Throwable] & Async) => Resource[F, A]`           |
| Instance   | `import KyoToCats.given` | `Async[Suspend]` where `Suspend[A] = A < (Abort[Throwable] & Async)` |

## Cats → Kyo

```scala
import cats.effect.IO as CatsIO
import kyo.*

// Plain IO. Argument is by-name, runs when the Kyo computation runs.
val v: Int < (Abort[Nothing] & Async) =
    CatsToKyo.get(CatsIO.pure(10))

// Mix with Kyo in one for-comprehension.
val doubled: Int < (Abort[Nothing] & Async) =
    for
        a <- CatsToKyo.get(CatsIO.pure(10))
        b <- Fiber.initUnscoped(2).map(_.get)
    yield a * b
```

Generic `F[_]` needs a `Dispatcher`:

```scala
import cats.effect.std.Dispatcher as CatsDispatcher

CatsDispatcher.parallel[CatsIO].use { disp =>
    given CatsDispatcher[CatsIO] = disp
    val v: Int < (Abort[Nothing] & Async) = CatsToKyo.lift(CatsIO.pure(7))
    KyoToCats.run(v)
}
```

## Kyo → Cats

```scala
import cats.effect.IO as CatsIO
import kyo.*

val kyoWork: Int < (Abort[Throwable] & Async) =
    Async.delay(10.millis)(42)

val asIO: CatsIO[Int] = KyoToCats.run(kyoWork)

// Generic F[_], needs Async[F]. Same for Resource.
val asGeneric: CatsIO[Int] = KyoToCats.runF[CatsIO, Int](kyoWork)
```

Only `Abort[Throwable] & Async` may be pending. Handle anything else first:

```scala
val withEnv: Int < (Env[Config] & Abort[Throwable] & Async) = ???
val asIO: CatsIO[Int] = KyoToCats.run(Env.run(Config(1000))(withEnv))
```

`import kyo.KyoToCats.given` provides `Async[Suspend]` so Kyo fills `F[_]` bounds in Cats libraries (http4s, fs2, doobie). Prefer native Kyo combinators inside Kyo code; use the instance at the boundary.

## Resources

Kyo `Scope` maps to Cats `Resource` in both directions. Acquire stays open for `Resource.use` / `Scope.run`; release runs on success, failure, or cancel. LIFO, exactly once. Partial-acquire failure releases what was already acquired.

```scala
import cats.effect.IO as CatsIO
import cats.effect.kernel.Resource as CatsResource
import kyo.*

// Kyo Scope -> Cats Resource
val scoped: Int < (Scope & Abort[Throwable] & Async) =
    Scope.acquireRelease(Sync.defer(42))(_ => Sync.defer(()))

val res: CatsResource[CatsIO, Int] = KyoToCats.runResource(scoped)
res.use(v => CatsIO.println(s"got $v"))

// Composes with Cats DSL
KyoToCats.runResource(scoped).map(_ + 1).use(_ => CatsIO.unit)
for
    a <- KyoToCats.runResource(scoped)
    b <- KyoToCats.runResource(scoped)
yield a + b
```

```scala
import cats.effect.IO as CatsIO
import cats.effect.kernel.Resource as CatsResource
import cats.effect.std.Dispatcher as CatsDispatcher
import kyo.*

CatsDispatcher.parallel[CatsIO].use { disp =>
    given CatsDispatcher[CatsIO] = disp
    val resource: CatsResource[CatsIO, Int] =
        CatsResource.make(CatsIO.pure(42))(_ => CatsIO.unit)
    KyoToCats.run {
        Scope.run {
            for
                a <- CatsToKyo.liftResource(resource)
                b <- Fiber.initUnscoped(2).map(_.get)
            yield a + b
        }
    }
}
```

`liftResource` needs `Dispatcher[F]` + `MonadCancel[F, Throwable]`. The `Resource` argument is by-name: nothing allocates until the Kyo computation runs.

## Errors

- Cats failure → Kyo `Panic`. `get`/`lift`/`liftResource` never produce `Abort.fail`; match `Result.Panic`.
- Kyo `Failure` and `Panic` → Cats error. `run`/`runF`/`runResource` collapse both into `F.raiseError`.
- Kyo logs finalizer failures (`Scope finalizer failed`) and lets `use`/`Scope.run` succeed. A failing release still ran; it just doesn't fail the scope.

## Cancellation

Bidirectional. Interrupting the Kyo fiber cancels the underlying `IO`/`F`; cancelling the Cats fiber interrupts the Kyo fiber and waits until it stops, so finalizers don't outlive the winner.

## Runtimes

- `get` runs the `IO` on `IORuntime.global`. A custom runtime elsewhere is not consulted.
- `lift`/`liftResource` submit through the `Dispatcher` in scope.
- `run`/`runF` start a `Fiber.initUnscoped` on Kyo's scheduler. The returned `IO`/`F` owns its lifetime.
