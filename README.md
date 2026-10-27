# kyo-cats-interop

Interop between [Kyo](https://github.com/getkyo/kyo) and the [Typelevel](https://typelevel.org) libraries Cats Effect and fs2.

Kyo dropped its Typelevel interop in [1.0.0-RC6](https://github.com/getkyo/kyo/pull/1779). This repository keeps those modules building against the published Kyo artifacts.

The code is derived from Kyo and is released under the same [Apache-2.0](LICENSE.txt) license.

**Latest release:** 0.1.3 (2026-10-27)

| Module               | Platforms                   | Scala          | What it does                                                                 |
| -------------------- | --------------------------- | -------------- | ---------------------------------------------------------------------------- |
| `kyo-cats`           | JVM, Scala.js               | 3.9.0          | Lift `cats.effect.IO` into Kyo and run Kyo as `IO`, incl. `Scope`/`Resource` |
| `kyo-scheduler-cats` | JVM                         | 3.9.0, 2.13.18 | Run Cats Effect on `kyo-scheduler`                                           |
| `kyo-compat-ce`      | JVM, Scala.js, Scala Native | 3.9.0          | `kyo.compat` backend on `cats.effect.IO` and fs2                             |

Built against:

| Dependency  | Version   |
| ----------- | --------- |
| Kyo         | 1.0.0-RC7 |
| Cats Effect | 3.7.1     |
| fs2         | 3.14.0    |

`fs2` is used only by `kyo-compat-ce`. On Scala.js, `kyo-compat-ce` also depends on [`scala-java-time`](https://github.com/cquiroz/scala-java-time) 2.7.0 because Scala.js does not ship `java.time`. `kyo-scheduler-cats` is the only module also built for Scala 2.13 (with `-Xsource:3`).

## kyo-cats

```scala
libraryDependencies += "io.github.netherite-systems" %%% "kyo-cats" % "0.1.3"
```

```scala
import cats.effect.IO as CatsIO
import kyo.*

val greeting: String < (Abort[Nothing] & Async) =
    for
        name <- Cats.get(CatsIO.pure("Ada"))
    yield s"Hello, $name"

val asIO: CatsIO[String] = Cats.run(greeting)
```

`Cats.get` schedules the `IO` on `IORuntime.global`. Failures from that `IO` surface as Kyo panics. `Cats.run` accepts only `Abort[Throwable] & Async`; handle any other effect first. Cancelling either side interrupts the other and waits until the interrupted side has stopped. Generic variants exist for arbitrary `F[_]` with `Async` (`Cats.runF`, `Cats.lift` via `Dispatcher`), and Kyo `Scope` maps to Cats `Resource` in both directions (`Cats.runResource`, `Cats.liftResource`). Full API and resource examples in [`kyo-cats/README.md`](kyo-cats/README.md).

## kyo-scheduler-cats

```scala
libraryDependencies += "io.github.netherite-systems" %% "kyo-scheduler-cats" % "0.1.3"
```

```scala
import cats.effect.IO
import kyo.KyoSchedulerIORuntime.global

val thread: String =
    IO.cede.map(_ => Thread.currentThread().getName).unsafeRunSync()
```

Import `KyoSchedulerIORuntime.global` where an `IORuntime` is summoned, or extend `KyoSchedulerIOApp` instead of `IOApp`. Compute and blocking both use `kyo.scheduler.Scheduler`. Timers stay on a two-thread scheduler owned by this runtime.

## kyo-compat-ce

This module is the Cats Effect backend for [`kyo-compat`](https://github.com/getkyo/kyo/tree/main/kyo-compat). It implements the same `kyo.compat.*` surface as Kyo's built-in backends (`CIO`, fibers, promises, channels, atomics, latches, meters, locals, and fs2 streams). A library written against `kyo.compat` can depend on this artifact instead of vendoring a binding.

Direct use:

```scala
libraryDependencies += "io.github.netherite-systems" %%% "kyo-compat-ce" % "0.1.3"
```

With `kyo-compat-plugin`, declare it as an external backend. The name `ce` matches the backend Kyo removed, so existing `-ce` artifact suffixes stay the same:

```scala
val CeLib = CompatBackendAxis.external(
    "ce",
    "Ce",
    "-ce",
    Set("jvm", "js", "native"),
    "io.github.netherite-systems",
    "kyo-compat-ce",
    "0.1.3"
)

lazy val myLib = (projectMatrix in file("my-lib"))
    .compatLibrary(CeLib)(VirtualAxis.jvm, VirtualAxis.js, VirtualAxis.native)(Seq("<scala-version>"))
```

`CIO.async` registers an `IO.async` cancellation token, so a timeout or an outer cancel can stop waiting on a callback. Cancelling that wait does not undo the external operation. `CIO.timeout` uses `Option`, so a successful `null` is `None`, the same result as a deadline. `CAtomicInt`, `CAtomicLong`, and `CAtomicBoolean` are separate opaque `Ref`s, each with its own arithmetic.

The cross-binding conformance suite is the one bundled in `kyo-compat-plugin`, unpacked per platform with `compatConformance()`. Bumping the plugin version with `kyoVersion` replaces that suite, so keep `kyo-compat-plugin` (in `project/plugins.sbt`) and `kyoVersion` (in `build.sbt`) on the same version.
