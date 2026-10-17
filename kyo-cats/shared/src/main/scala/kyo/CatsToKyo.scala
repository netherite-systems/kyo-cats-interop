package kyo

import cats.effect.IO as CatsIO
import cats.effect.kernel.MonadCancel as CatsMonadCancel
import cats.effect.kernel.Resource as CatsResource
import cats.effect.std.Dispatcher as CatsDispatcher
import kyo.kernel.*
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

private[kyo] trait CatsToKyo:

    /** Lifts a `cats.effect.IO` into a Kyo effect. The `IO` argument is by-name, so construction is deferred until the Kyo computation
      * runs. Failures from the `IO` are Kyo panics, not `Abort` failures.
      *
      * See https://typelevel.org/cats-effect/api/3.x/cats/effect/IO.html
      */
    def get[A](io: => CatsIO[A])(using Frame): A < (Abort[Nothing] & Async) =
        Sync.Unsafe.defer {
            import cats.effect.unsafe.implicits.global
            val p                = Promise.Unsafe.init[A, Any]()
            val (future, cancel) = io.unsafeToFutureCancelable()
            val interrupted      = new java.util.concurrent.atomic.AtomicBoolean(false)
            future.onComplete {
                case Success(v) => p.complete(Result.succeed(v))
                // A failure caused by our own cancellation (the Kyo fiber was interrupted) is
                // not a panic: the interrupt settles the promise, so the boxed cancellation
                // exception must not race in and leak as an unexpected panic.
                // See https://typelevel.org/cats-effect/api/3.x/cats/effect/IO.html
                case Failure(ex) => if !interrupted.get() then discard(p.complete(Result.panic(ex)))
            }(using ExecutionContext.parasitic)
            p.onInterrupt { _ =>
                interrupted.set(true)
                discard(cancel())
            }
            p.safe.get
        }
    end get

    /** Lifts an arbitrary `F[A]` into Kyo via a `Dispatcher`. Failures become Kyo panics, mirroring `get`.
      *
      * Requires a `Dispatcher[F]` in scope (typically from `Dispatcher.parallel[F]`). This is the minimal runtime handle that can run an
      * arbitrary `F` outside its own context; `Async[F]` alone cannot submit work from impure code.
      */
    def lift[F[_], A](fa: => F[A])(using CatsDispatcher[F], Frame): A < (Abort[Nothing] & Async) =
        Sync.Unsafe.defer {
            val dispatcher       = summon[CatsDispatcher[F]]
            val faValue          = fa
            val (future, cancel) = dispatcher.unsafeToFutureCancelable(faValue)
            val promise          = Promise.Unsafe.init[A, Any]()
            bridgeFuture(future, cancel)(
                onInterrupt => promise.onInterrupt(_ => onInterrupt()),
                value => promise.complete(Result.succeed(value)),
                error => promise.complete(Result.panic(error))
            )
            promise.safe.get
        }
    end lift

    /** Lifts a `cats.effect.kernel.Resource[F, A]` into a Kyo computation that requires `Scope`. Allocation runs via `lift`
      * (so it needs a `Dispatcher[F]`), and release is registered with `Scope.acquireRelease` so it runs when the Kyo scope
      * closes. The `Resource` argument is by-name, so construction is deferred until the Kyo computation runs. Failures
      * (including allocation failures) become Kyo panics, mirroring `lift`.
      *
      * See https://typelevel.org/cats-effect/api/3.x/cats/effect/kernel/Resource.html
      */
    def liftResource[F[_], A](resource: => CatsResource[F, A])(using
        CatsDispatcher[F],
        CatsMonadCancel[F, Throwable],
        Frame
    ): A < (Scope & Abort[Nothing] & Async) =
        Scope.acquireRelease(lift(resource.allocated))((pair: (A, F[Unit])) => lift(pair._2)).map(_._1)
    end liftResource

    private def bridgeFuture[A](future: Future[A], cancel: () => Future[Unit])(
        registerInterrupt: (() => Unit) => Unit,
        completeSuccess: A => Boolean,
        completeFailure: Throwable => Boolean
    ): Unit =
        val interrupted = new java.util.concurrent.atomic.AtomicBoolean(false)
        future.onComplete {
            case Success(v) => discard(completeSuccess(v))
            // A failure caused by our own cancellation (the Kyo fiber was interrupted) is not a panic: the interrupt settles the promise,
            // so the boxed cancellation exception must not race in and leak as an unexpected panic.
            case Failure(ex) => if !interrupted.get() then discard(completeFailure(ex))
        }(using ExecutionContext.parasitic)
        registerInterrupt { () =>
            interrupted.set(true)
            discard(cancel())
        }
    end bridgeFuture

end CatsToKyo
