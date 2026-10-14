package kyo

import cats.effect.IO as CatsIO
import kyo.kernel.*
import scala.concurrent.ExecutionContext
import scala.util.Failure
import scala.util.Success

object Cats:

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

    /** Interprets a Kyo computation as `cats.effect.IO`.
      *
      * Only `Abort[Throwable] & Async` may be pending. Cancellation waits until the Kyo fiber has finished, so finalizers do not outlive
      * the `IO`. The join is installed under `IO.uncancelable` so an interrupt that arrives before the async callback is registered still
      * interrupts the Kyo fiber. The join wait returns `Some(IO.unit)` so that wait stays cancelable and `onCancel` can run; the finalizer
      * itself waits with no token, because it must run to completion.
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
                                // `None` masks this wait, so a cancel that arrives after registration never reaches `onCancel` and the Kyo
                                // fiber keeps running. `Some(IO.unit)` keeps the wait cancelable when there is nothing else to finalize.
                                // See https://typelevel.org/cats-effect/api/3.x/cats/effect/IO$.html
                                Some(CatsIO.unit)
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
