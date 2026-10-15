package kyo

import cats.~>
import cats.arrow.FunctionK
import cats.effect.IO as CatsIO
import cats.effect.kernel.Async as CatsAsync
import cats.effect.kernel.Cont as CatsCont
import cats.effect.kernel.Deferred as CatsDeferred
import cats.effect.kernel.MonadCancel as CatsMonadCancel
import cats.effect.kernel.Outcome as CatsOutcome
import cats.effect.kernel.Poll as CatsPoll
import cats.effect.kernel.Ref as CatsRef
import cats.effect.kernel.Sync as CatsSync
import cats.effect.kernel.Unique as CatsUnique
import cats.effect.std.Dispatcher as CatsDispatcher
import kyo.kernel.*
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success

object Cats:

    /** Kyo suspension that carries a typed Cats failure channel. This is the `F[_]` that the typeclass instance below implements, and the
      * shape `runF` accepts.
      */
    type Suspend[A] = A < (Abort[Throwable] & Async)

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

    /** Interprets a Kyo computation as an arbitrary `F[_]`. This is the generic form of `run`: the only requirement is `Async[F]`, which
      * is the minimal Cats Effect capability that can suspend an async callback (`async`), restore cancelability (`poll`), and attach a
      * finalizer (`onCancel`).
      *
      * Cancellation mirrors `run`: cancelling the `F` fiber interrupts the Kyo fiber and waits (uncancelably) until it has stopped.
      */
    def runF[F[_], A](v: => A < (Abort[Throwable] & Async))(using F: CatsAsync[F], frame: Frame): F[A] =
        F.uncancelable { poll =>
            F.defer {
                import AllowUnsafe.embrace.danger
                val fiber: Fiber[A, Abort[Throwable]] =
                    Sync.Unsafe.evalOrThrow(Fiber.initUnscoped(v))(using frame, AllowUnsafe.embrace.danger)
                val join: F[A] =
                    F.async[A] { cb =>
                        F.delay {
                            fiber.unsafe.onComplete {
                                case Result.Success(a) => cb(Right(a.eval))
                                case Result.Failure(e) => cb(Left(e))
                                case Result.Panic(e)   => cb(Left(e))
                            }
                            Some(F.unit)
                        }
                    }
                val interruptAndAwait: F[Unit] =
                    F.async[Unit] { k =>
                        F.delay {
                            fiber.unsafe.onComplete(_ => k(Right(())))
                            discard(fiber.unsafe.interrupt()(using frame, AllowUnsafe.embrace.danger))
                            None
                        }
                    }
                F.onCancel(poll(join), interruptAndAwait)
            }
        }
    end runF

    /** Cats Effect `Async` instance for `Suspend`, so Kyo computations can fill an `F[_]` bound in Cats libraries (http4s, fs2, doobie,
      * ...). By subtyping it also serves every `Sync`/`Temporal`/`Concurrent`/`MonadCancel` bound. Import with `import kyo.Cats.given`.
      *
      * Mapping: `pure`/`map`/`flatMap` are Kyo sequencing; `raiseError` is `Abort.fail`; `handleErrorWith` recovers both Kyo `Failure` and
      * `Panic` since Cats has a single error channel; `delay`/`suspend` are `Sync.defer`; async/cancelable/concurrency/clock operations
      * delegate to the `IO` instance through `run`/`liftTypedIO` so masking, finalizers, and timers inherit Cats Effect semantics. Each
      * delegation crosses the runtime boundary, so prefer native Kyo combinators inside Kyo code and reserve the instance for the boundary
      * where a Cats API demands an `F`.
      */
    given asyncSuspend: CatsAsync[Suspend] with
        private given frameInstance: Frame     = Frame.internal
        private val ioAsync: CatsAsync[CatsIO] = CatsAsync[CatsIO]

        private def toIO[A](fa: Suspend[A]): CatsIO[A]    = Cats.run(fa)(using frameInstance)
        private def fromIO[A](ioa: CatsIO[A]): Suspend[A] = Sync.Unsafe.defer {
            import cats.effect.unsafe.implicits.global
            val (future, cancel) = ioa.unsafeToFutureCancelable()
            val promise          = Promise.Unsafe.init[A, Abort[Throwable]]()
            bridgeFuture(future, cancel)(
                onInterrupt => promise.onInterrupt(_ => onInterrupt()),
                value => promise.complete(Result.succeed(value)),
                error => promise.complete(Result.fail(error))
            )
            promise.safe.get
        }

        private val ioToSuspend: FunctionK[CatsIO, Suspend] = new FunctionK[CatsIO, Suspend]:
            def apply[A](fa: CatsIO[A]): Suspend[A] = fromIO(fa)

        private def toOutcomeKyo[A](oc: CatsOutcome[CatsIO, Throwable, A]): CatsOutcome[Suspend, Throwable, A] =
            oc match
                case CatsOutcome.Succeeded(fa) => CatsOutcome.Succeeded(fromIO(fa))
                case CatsOutcome.Errored(e)    => CatsOutcome.Errored(e)
                case CatsOutcome.Canceled()    => CatsOutcome.Canceled()

        private def toKyoFiber[A](ioFiber: cats.effect.kernel.Fiber[CatsIO, Throwable, A])
            : cats.effect.kernel.Fiber[Suspend, Throwable, A] =
            new cats.effect.kernel.Fiber[Suspend, Throwable, A]:
                def cancel: Suspend[Unit]                             = fromIO(ioFiber.cancel)
                def join: Suspend[CatsOutcome[Suspend, Throwable, A]] = fromIO(ioFiber.join.map(toOutcomeKyo))

        // -- Monad --
        def pure[A](x: A): Suspend[A]                                       = x
        override def map[A, B](fa: Suspend[A])(f: A => B): Suspend[B]       = fa.map(f)
        def flatMap[A, B](fa: Suspend[A])(f: A => Suspend[B]): Suspend[B]   = fa.flatMap(f)
        def tailRecM[A, B](a: A)(f: A => Suspend[Either[A, B]]): Suspend[B] =
            f(a).flatMap {
                case Left(next)  => tailRecM(next)(f)
                case Right(done) => done
            }

        // -- MonadError (single Throwable channel: Failure and Panic both recoverable) --
        def raiseError[A](e: Throwable): Suspend[A]                                    = Abort.fail(e)
        def handleErrorWith[A](fa: Suspend[A])(f: Throwable => Suspend[A]): Suspend[A] =
            Abort.run[Throwable](fa).flatMap {
                case Result.Success(a) => a
                case Result.Failure(e) => f(e)
                case Result.Panic(e)   => f(e)
            }

        // -- MonadCancel --
        def forceR[A, B](fa: Suspend[A])(fb: Suspend[B]): Suspend[B] =
            fromIO(ioAsync.forceR(toIO(fa))(toIO(fb)))
        def uncancelable[A](body: CatsPoll[Suspend] => Suspend[A]): Suspend[A] =
            fromIO(ioAsync.uncancelable((pollIO: CatsPoll[CatsIO]) =>
                val suspendedPoll = new CatsPoll[Suspend]:
                    def apply[B](fa: Suspend[B]): Suspend[B] = fromIO(pollIO(toIO(fa)))
                toIO(body(suspendedPoll))
            ))
        def canceled: Suspend[Unit] =
            fromIO(ioAsync.canceled)
        def onCancel[A](fa: Suspend[A], fin: Suspend[Unit]): Suspend[A] =
            fromIO(ioAsync.onCancel(toIO(fa), toIO(fin)))

        // -- Sync --
        def suspend[A](hint: CatsSync.Type)(thunk: => A): Suspend[A] =
            hint match
                case CatsSync.Type.Delay | CatsSync.Type.Blocking =>
                    Sync.defer(thunk)
                case _ =>
                    fromIO(ioAsync.suspend(hint)(thunk))
        override def delay[A](thunk: => A): Suspend[A]             = Sync.defer(thunk)
        override def blocking[A](thunk: => A): Suspend[A]          = fromIO(CatsIO.blocking(thunk))
        override def interruptible[A](thunk: => A): Suspend[A]     = fromIO(CatsIO.interruptible(thunk))
        override def interruptibleMany[A](thunk: => A): Suspend[A] = fromIO(CatsIO.interruptibleMany(thunk))

        // -- Unique / Clock --
        override def unique: Suspend[CatsUnique.Token] =
            fromIO(ioAsync.unique)
        def monotonic: Suspend[FiniteDuration] =
            fromIO(ioAsync.monotonic)
        def realTime: Suspend[FiniteDuration] =
            fromIO(ioAsync.realTime)

        // -- GenSpawn --
        def cede: Suspend[Unit]                                                                = fromIO(ioAsync.cede)
        override def never[A]: Suspend[A]                                                      = Async.never
        def start[A](fa: Suspend[A]): Suspend[cats.effect.kernel.Fiber[Suspend, Throwable, A]] =
            fromIO(ioAsync.start(toIO(fa)).map(toKyoFiber(_)))
        override def racePair[A, B](fa: Suspend[A], fb: Suspend[B]): Suspend[Either[
            (CatsOutcome[Suspend, Throwable, A], cats.effect.kernel.Fiber[Suspend, Throwable, B]),
            (cats.effect.kernel.Fiber[Suspend, Throwable, A], CatsOutcome[Suspend, Throwable, B])
        ]] =
            fromIO(ioAsync.racePair(toIO(fa), toIO(fb)).map {
                case Left((oca, fibB))  => Left((toOutcomeKyo(oca), toKyoFiber(fibB)))
                case Right((fibA, ocb)) => Right((toKyoFiber(fibA), toOutcomeKyo(ocb)))
            })

        // -- GenTemporal --
        def sleep(time: FiniteDuration): Suspend[Unit] =
            fromIO(ioAsync.sleep(time))

        // -- GenConcurrent (via mapK so Ref/Deferred semantics stay with IO) --
        def ref[A](a: A): Suspend[CatsRef[Suspend, A]] =
            fromIO(ioAsync.ref(a).map(_.mapK(ioToSuspend)))
        def deferred[A]: Suspend[CatsDeferred[Suspend, A]] =
            fromIO(ioAsync.deferred[A].map(_.mapK(ioToSuspend)))

        // -- Async --
        override def asyncCheckAttempt[A](k: (Either[Throwable, A] => Unit) => Suspend[Either[Option[Suspend[Unit]], A]]): Suspend[A] =
            fromIO(ioAsync.asyncCheckAttempt(cb =>
                toIO(k(cb)).map {
                    case Right(a)        => Right(a)
                    case Left(Some(fin)) => Left(Some(toIO(fin)))
                    case Left(None)      => Left(None)
                }
            ))
        override def async[A](k: (Either[Throwable, A] => Unit) => Suspend[Option[Suspend[Unit]]]): Suspend[A] =
            fromIO(ioAsync.async(cb => toIO(k(cb)).map(_.map(toIO(_)))))
        override def async_[A](k: (Either[Throwable, A] => Unit) => Unit): Suspend[A] =
            fromIO(ioAsync.async_(k))
        def evalOn[A](fa: Suspend[A], ec: ExecutionContext): Suspend[A] =
            fromIO(ioAsync.evalOn(toIO(fa), ec))
        def executionContext: Suspend[ExecutionContext] =
            fromIO(ioAsync.executionContext)
        def cont[K, R](body: CatsCont[Suspend, K, R]): Suspend[R] =
            fromIO(ioAsync.cont(new CatsCont[CatsIO, K, R]:
                def apply[G[_]](implicit G: CatsMonadCancel[G, Throwable]): (Either[Throwable, K] => Unit, G[K], CatsIO ~> G) => G[R] =
                    (resume, get, liftIO) =>
                        val liftKyo: Suspend ~> G =
                            new (Suspend ~> G):
                                def apply[A](fa: Suspend[A]): G[A] = liftIO(toIO(fa))
                        body.apply[G](using G)(resume, get, liftKyo)))

        override def applicative: cats.Applicative[Suspend] = this
    end asyncSuspend
end Cats
