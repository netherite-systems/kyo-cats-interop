package kyo

import cats.effect.IO as CatsIO
import scala.concurrent.ExecutionContext
import scala.util.Failure
import scala.util.Success

/** Bridge between Kyo and `cats.effect.IO`.
  *
  * `get` lifts an `IO` into `A < (Abort[Nothing] & Async)`. Failures from the `IO` are Kyo panics, not `Abort` failures. The lifted `IO`
  * runs on `IORuntime.global`, and interrupting the Kyo fiber cancels that `IO`.
  *
  * See https://typelevel.org/cats-effect/api/3.x/cats/effect/unsafe/IORuntime.html
  */
object Cats:

    /** Lifts `io` into a Kyo computation. The by-name argument is deferred until the computation runs.
      *
      * A failure produced by cancelling the Kyo fiber is dropped: the interrupt settles the promise, so the cancellation exception must not
      * also complete it as a panic.
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
                case Success(v) =>
                    p.completeDiscard(Result.succeed(v))
                case Failure(ex) =>
                    if !interrupted.get() then p.completeDiscard(Result.panic(ex))
            }(using ExecutionContext.parasitic)
            // Interrupting the Kyo promise cancels the running `IO`.
            // See https://getkyo.io/latest/kyo-core/
            p.onInterrupt { _ =>
                interrupted.set(true)
                discard(cancel())
            }
            p.safe.get
        }
    end get

    /** Interprets a Kyo computation as `cats.effect.IO`.
      *
      * Only `Abort[Throwable] & Async` may be pending. Cancellation waits until the Kyo fiber has finished, so finalizers do not outlive
      * the `IO`. The join is installed under `IO.uncancelable` so an interrupt that arrives before the async callback is registered still
      * interrupts the Kyo fiber.
      *
      * See https://typelevel.org/cats-effect/docs/typeclasses/monadcancel
      */
    def run[A](v: => A < (Abort[Throwable] & Async))(using frame: Frame): CatsIO[A] =
        CatsIO.uncancelable { poll =>
            CatsIO.defer {
                import AllowUnsafe.embrace.danger
                Fiber.initUnscoped(v).map { fiber =>
                    val join: CatsIO[A] =
                        CatsIO.async[A] { cb =>
                            CatsIO {
                                fiber.unsafe.onComplete {
                                    case Result.Success(a) => cb(Right(a.eval))
                                    case Result.Failure(e) => cb(Left(e))
                                    case Result.Panic(e)   => cb(Left(e))
                                }
                                None
                            }
                        }
                    val interruptAndAwait: CatsIO[Unit] =
                        CatsIO.async[Unit] { k =>
                            CatsIO {
                                fiber.unsafe.onComplete(_ => k(Right(())))
                                discard(fiber.unsafe.interrupt())
                                None
                            }
                        }
                    poll(join).onCancel(interruptAndAwait)
                }.handle(Sync.Unsafe.evalOrThrow)
            }
        }
    end run
end Cats
