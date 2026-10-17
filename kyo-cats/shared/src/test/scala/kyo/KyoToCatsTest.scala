package kyo

import cats.effect.IO as CatsIO
import cats.effect.kernel.Resource as CatsResource
import cats.effect.unsafe.implicits.global
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kyo.*
import kyo.internal.Platform

class KyoToCatsTest extends kyo.test.Test[Any]:

    // Run the body through the Cats Effect runtime, then bridge the resulting Future back into Kyo.
    // See https://typelevel.org/cats-effect/api/3.x/cats/effect/IO.html
    def runCatsIO[T](v: CatsIO[T]): T < Async =
        Async.fromFuture(v.unsafeToFuture())

    def runKyo(v: => Unit < (Abort[Nothing] & Async)): Unit < Async =
        Async.fromFuture(KyoToCats.run(v).unsafeToFuture())

    "fibers" - {
        "nested Kyo in Cats" in runKyo {
            CatsToKyo.get(CatsIO.defer {
                val kyoComputation = Fiber.initUnscoped(42).map(_.get)
                KyoToCats.run(kyoComputation)
            }).map(v => assert(v == 42))
        }
    }

    "interrupts" - {

        def kyoLoop(started: CountDownLatch, done: CountDownLatch): Unit < Sync =
            def loop(i: Int): Unit < Sync =
                Sync.defer {
                    if i == 0 then
                        Sync.defer(started.countDown()).andThen(loop(i + 1))
                    else
                        loop(i + 2)
                }
            Sync.ensure(Sync.defer(done.countDown()))(loop(0))
        end kyoLoop

        def catsLoop(started: CountDownLatch, done: CountDownLatch): CatsIO[Unit] =
            def loop(i: Int): CatsIO[Unit] =
                CatsIO.defer {
                    if i == 0 then
                        CatsIO(started.countDown())
                            .flatMap(_ => loop(i + 1))
                    else
                        loop(i + 1)
                }
            loop(0).guarantee(CatsIO(done.countDown()))
        end catsLoop

        if Platform.isJVM then

            "runCatsIO" - {
                "cats to kyo" in runCatsIO {
                    val started = new CountDownLatch(1)
                    val done    = new CountDownLatch(1)
                    for
                        f <- KyoToCats.run(kyoLoop(started, done)).start
                        _ <- CatsIO(assert(started.await(5, TimeUnit.SECONDS), "loop did not start"))
                        _ <- f.cancel
                        r <- f.join
                        _ <- CatsIO(assert(done.await(5, TimeUnit.SECONDS), "finalizer did not run"))
                    yield assert(r.isCanceled)
                    end for
                }

                "both" in runCatsIO {
                    val started = new CountDownLatch(2)
                    val done    = new CountDownLatch(2)
                    val v       =
                        Async.zip[Throwable, Unit, Unit, Any](
                            CatsToKyo.get(catsLoop(started, done)),
                            kyoLoop(started, done)
                        )
                    for
                        f <- KyoToCats.run(v).start
                        _ <- CatsIO(assert(started.await(5, TimeUnit.SECONDS), "loops did not start"))
                        _ <- f.cancel
                        r <- f.join
                        _ <- CatsIO(assert(done.await(5, TimeUnit.SECONDS), "finalizers did not run"))
                    yield assert(r.isCanceled)
                    end for
                }

                "parallel loops" in runCatsIO {
                    val started        = new CountDownLatch(2)
                    val done           = new CountDownLatch(2)
                    def parallelEffect =
                        KyoToCats.run {
                            val loop1 = CatsToKyo.get(catsLoop(started, done))
                            val loop2 = kyoLoop(started, done)
                            Async.zip[Throwable, Unit, Unit, Any](loop1, loop2)
                        }
                    for
                        f <- parallelEffect.start
                        _ <- CatsIO(assert(started.await(5, TimeUnit.SECONDS), "loops did not start"))
                        _ <- f.cancel
                        r <- f.join
                        _ <- CatsIO(assert(done.await(5, TimeUnit.SECONDS), "finalizers did not run"))
                    yield assert(r.isCanceled)
                    end for
                }

                "race loops" in runCatsIO {
                    val started    = new CountDownLatch(2)
                    val done       = new CountDownLatch(2)
                    def raceEffect =
                        KyoToCats.run {
                            val loop1 = CatsToKyo.get(catsLoop(started, done))
                            val loop2 = kyoLoop(started, done)
                            Async.race[Throwable, Unit, Any](loop1, loop2)
                        }
                    for
                        f <- raceEffect.start
                        _ <- CatsIO(assert(started.await(5, TimeUnit.SECONDS), "loops did not start"))
                        _ <- f.cancel
                        r <- f.join
                        _ <- CatsIO(assert(done.await(5, TimeUnit.SECONDS), "finalizers did not run"))
                    yield assert(r.isCanceled)
                    end for
                }
            }
        end if
    }

    "F-generic bridges" - {
        "runF generic to IO" in runCatsIO {
            val kyoWork: Int < (Abort[Throwable] & Async) = Async.delay(1.millis)(42)
            KyoToCats.runF[CatsIO, Int](kyoWork).map(v => assert(v == 42))
        }

        "runF generic to EitherT" in runCatsIO {
            import cats.data.EitherT
            type Eith[A] = EitherT[CatsIO, Throwable, A]
            val kyoWork: Int < (Abort[Throwable] & Async) = 99
            val eff: Eith[Int]                            =
                KyoToCats.runF[Eith, Int](kyoWork)
            eff.value.flatMap {
                case Right(v) => CatsIO(assert(v == 99))
                case Left(e)  => CatsIO.raiseError(e)
            }
        }

        "runF maps Abort.fail to F error" in runCatsIO {
            val boom                                      = new RuntimeException("kyo abort")
            val kyoWork: Int < (Abort[Throwable] & Async) = Abort.fail(boom)
            KyoToCats.runF[CatsIO, Int](kyoWork).attempt.map {
                case Left(ex) => assert(ex == boom)
                case Right(v) => fail(s"expected error, got $v")
            }
        }

        "runF maps Kyo panic to F error" in runCatsIO {
            val boom                                       = new RuntimeException("kyo panic")
            val kyoPanic: Int < (Abort[Throwable] & Async) = CatsToKyo.get(CatsIO.raiseError[Int](boom))
            KyoToCats.runF[CatsIO, Int](kyoPanic).attempt.map {
                case Left(ex) => assert(ex == boom)
                case Right(v) => fail(s"expected error, got $v")
            }
        }

        if Platform.isJVM then
            "cancellation across the generic boundary" - {
                "canceling outer Cats fiber cancels lifted F" in runCatsIO {
                    import cats.effect.std.Dispatcher as CatsDispatcher
                    val started = new CountDownLatch(1)
                    val done    = new CountDownLatch(1)
                    CatsDispatcher.parallel[CatsIO].use { disp =>
                        given CatsDispatcher[CatsIO] = disp
                        def effect                   =
                            KyoToCats.run {
                                val looped = CatsToKyo.lift(CatsIO(started.countDown()) >> CatsIO.never.guarantee(CatsIO(done.countDown())))
                                Async.zip[Throwable, Unit, Unit, Any](looped, Async.never[Unit])
                            }
                        for
                            f <- effect.start
                            _ <- CatsIO(assert(started.await(5, TimeUnit.SECONDS), "lifted effect did not start"))
                            _ <- f.cancel
                            r <- f.join
                            _ <- CatsIO(assert(done.await(5, TimeUnit.SECONDS), "lifted finalizer did not run"))
                        yield assert(r.isCanceled)
                        end for
                    }
                }
            }
        end if
    }

    "minimal typeclasses" - {
        import kyo.KyoToCats.given

        def runKyoT(v: => Unit < (Abort[Throwable] & Async)): Unit < Async =
            Async.fromFuture(KyoToCats.run(v).unsafeToFuture())

        "Async summon resolves Sync/Temporal bounds" in runKyoT {
            def needsSync[F[_]](using cats.effect.kernel.Sync[F]): F[Int]   = summon[cats.effect.kernel.Sync[F]].pure(3)
            def needsAsync[F[_]](using cats.effect.kernel.Async[F]): F[Int] = summon[cats.effect.kernel.Async[F]].pure(4)
            for
                a <- needsSync[KyoToCats.Suspend]
                b <- needsAsync[KyoToCats.Suspend]
            yield assert(a == 3 && b == 4)
            end for
        }

        "MonadError recovers Failure and Panic" in runKyoT {
            val F                                             = summon[cats.effect.kernel.Async[KyoToCats.Suspend]]
            val fromFailure: Int < (Abort[Throwable] & Async) = F.raiseError[Int](new RuntimeException("f"))
            val fromAbort: Int < (Abort[Throwable] & Async)   = Abort.fail(new RuntimeException("q"))
            for
                r1 <- Abort.run[Throwable](F.handleErrorWith(fromFailure)(_ => 1))
                r2 <- Abort.run[Throwable](F.handleErrorWith(fromAbort)(_ => 2))
            yield
                assert(r1 == Result.succeed(1))
                assert(r2 == Result.succeed(2))
            end for
        }

        "handleErrorWith recovers Panic" in runKyoT {
            val F                                 = summon[cats.effect.kernel.Async[KyoToCats.Suspend]]
            val panicking: KyoToCats.Suspend[Int] = CatsToKyo.get(CatsIO.raiseError[Int](new RuntimeException("boom")))
            Abort.run[Throwable](F.handleErrorWith(panicking)(_ => 7)).map(r => assert(r == Result.succeed(7)))
        }

        "start and join through instance" in runKyoT {
            val F = summon[cats.effect.kernel.Async[KyoToCats.Suspend]]
            for
                fib <- F.start(F.pure(42))
                v   <- fib.joinWithNever
            yield assert(v == 42)
            end for
        }

        "async roundtrip through instance" in runKyoT {
            val F = summon[cats.effect.kernel.Async[KyoToCats.Suspend]]
            F.async_[Int](cb => cb(Right(5))).map(v => assert(v == 5))
        }

        "Ref/Deferred roundtrip through instance" in runKyoT {
            val F = summon[cats.effect.kernel.Async[KyoToCats.Suspend]]
            for
                ref <- F.ref(0)
                _   <- ref.update(_ + 1)
                v   <- ref.get
                d   <- F.deferred[String]
                _   <- d.complete("ok")
                s   <- d.get
            yield assert(v == 1 && s == "ok")
            end for
        }

        "sleep/canceled/uncancelable via instance" in runKyoT {
            val F = summon[cats.effect.kernel.Async[KyoToCats.Suspend]]
            for
                _ <- F.sleep(scala.concurrent.duration.FiniteDuration(1, "ms"))
                _ <- F.uncancelable(_ => 5)
                _ <- F.onCancel(6, F.unit)
            yield assert(true)
            end for
        }
    }

    "Resource bridges" - {
        "acquire and release via Resource.use" in runCatsIO {
            val acquired                                         = new AtomicInteger(0)
            val released                                         = new AtomicInteger(0)
            val scoped: Int < (Scope & Abort[Throwable] & Async) =
                Scope.acquireRelease(Sync.defer { acquired.incrementAndGet(); 42 })((_: Int) =>
                    Sync.defer { released.incrementAndGet(); () }
                )
            KyoToCats.runResource[CatsIO, Int](scoped).use { v =>
                CatsIO(assert(v == 42)) >> CatsIO(assert(acquired.get() == 1)) >> CatsIO(assert(released.get() == 0))
            } >> CatsIO(assert(released.get() == 1))
        }

        "multiple scoped resources release" in runCatsIO {
            val released                                                 = new AtomicInteger(0)
            def scoped(i: Int): Int < (Scope & Abort[Throwable] & Async) =
                Scope.acquireRelease(Sync.defer(i))((_: Int) => Sync.defer { released.incrementAndGet(); () })
            val combined: (Int, Int) < (Scope & Abort[Throwable] & Async) =
                for
                    a <- scoped(1)
                    b <- scoped(2)
                yield (a, b)
            KyoToCats.runResource[CatsIO, (Int, Int)](combined).use { case (a, b) =>
                CatsIO(assert(a == 1 && b == 2))
            } >> CatsIO(assert(released.get() == 2))
        }

        "scoped value composes with fibers" in runCatsIO {
            val scoped: Int < (Scope & Abort[Throwable] & Async) =
                Scope.acquireRelease(Sync.defer(1))((_: Int) => Sync.defer(()))
            KyoToCats.runResource[CatsIO, Int] {
                for
                    a <- scoped
                    b <- Fiber.initUnscoped(2).map(_.get)
                yield a + b
            }.use(v => CatsIO(assert(v == 3)))
        }

        "Abort.fail becomes Resource error" in runCatsIO {
            val boom                                             = new RuntimeException("kyo abort")
            val scoped: Int < (Scope & Abort[Throwable] & Async) = Abort.fail(boom)
            KyoToCats.runResource[CatsIO, Int](scoped).use(_ => CatsIO.unit).attempt.map {
                case Left(ex) => assert(ex == boom)
                case Right(_) => fail("expected error")
            }
        }

        "panic becomes Resource error" in runCatsIO {
            val boom                                             = new RuntimeException("kyo panic")
            val scoped: Int < (Scope & Abort[Throwable] & Async) = CatsToKyo.get(CatsIO.raiseError[Int](boom))
            KyoToCats.runResource[CatsIO, Int](scoped).use(_ => CatsIO.unit).attempt.map {
                case Left(ex) => assert(ex == boom)
                case Right(_) => fail("expected error")
            }
        }

        "release runs when use fails" in runCatsIO {
            val boom                                             = new RuntimeException("use boom")
            val released                                         = new AtomicInteger(0)
            val scoped: Int < (Scope & Abort[Throwable] & Async) =
                Scope.acquireRelease(Sync.defer(7))((_: Int) => Sync.defer { released.incrementAndGet(); () })
            KyoToCats.runResource[CatsIO, Int](scoped).use(_ => CatsIO.raiseError[Unit](boom)).attempt.flatMap {
                case Left(ex) => CatsIO(assert(ex == boom)) >> CatsIO(assert(released.get() == 1))
                case Right(_) => CatsIO(fail("expected error"))
            }
        }

        "runResource generic to EitherT" in runCatsIO {
            import cats.data.EitherT
            type Eith[A] = EitherT[CatsIO, Throwable, A]
            val scoped: Int < (Scope & Abort[Throwable] & Async) =
                Scope.acquireRelease(Sync.defer(99))((_: Int) => Sync.defer(()))
            val res: CatsResource[Eith, Int] = KyoToCats.runResource[Eith, Int](scoped)
            res.use(v => EitherT.liftF(CatsIO(assert(v == 99)))).value.flatMap {
                case Right(_) => CatsIO.unit
                case Left(e)  => CatsIO.raiseError(e)
            }
        }

        "partial acquire failure releases prior scopes (Abort)" in runCatsIO {
            val releasedOk                                         = new AtomicInteger(0)
            val releasedFail                                       = new AtomicInteger(0)
            val boom                                               = new RuntimeException("second acquire abort")
            def scopedOk: Int < (Scope & Abort[Throwable] & Async) =
                Scope.acquireRelease(Sync.defer(1))((_: Int) => Sync.defer { releasedOk.incrementAndGet(); () })
            def scopedFail: Int < (Scope & Abort[Throwable] & Async) =
                Scope.acquireRelease((Abort.fail(boom): Int < Abort[Throwable]))((_: Int) =>
                    Sync.defer { releasedFail.incrementAndGet(); () }
                )
            val combined: Int < (Scope & Abort[Throwable] & Async) =
                for
                    a <- scopedOk
                    b <- scopedFail
                yield a + b
            KyoToCats.runResource[CatsIO, Int](combined).use(_ => CatsIO.unit).attempt.flatMap {
                case Left(ex) =>
                    CatsIO(assert(ex == boom)) >>
                        CatsIO(assert(releasedOk.get() == 1, "prior scope was not released")) >>
                        CatsIO(assert(releasedFail.get() == 0, "failed acquire must not release"))
                case Right(_) => CatsIO(fail("expected error"))
            }
        }

        "partial acquire failure releases prior scopes (panic)" in runCatsIO {
            val releasedOk                                         = new AtomicInteger(0)
            val boom                                               = new RuntimeException("second acquire panic")
            def scopedOk: Int < (Scope & Abort[Throwable] & Async) =
                Scope.acquireRelease(Sync.defer(1))((_: Int) => Sync.defer { releasedOk.incrementAndGet(); () })
            val combined: Int < (Scope & Abort[Throwable] & Async) =
                for
                    a <- scopedOk
                    b <- CatsToKyo.get(CatsIO.raiseError[Int](boom))
                yield a + b
            KyoToCats.runResource[CatsIO, Int](combined).use(_ => CatsIO.unit).attempt.flatMap {
                case Left(ex) =>
                    CatsIO(assert(ex == boom)) >>
                        CatsIO(assert(releasedOk.get() == 1, "prior scope was not released"))
                case Right(_) => CatsIO(fail("expected error"))
            }
        }

        // Kyo logs Scope finalizer failures ("Scope finalizer failed") instead of failing the
        // close/await that backs the Cats release, so `use` still succeeds. This pins that behavior.
        "release failure does not fail use" in runCatsIO {
            val releaseBoom                                      = new RuntimeException("release boom")
            val released                                         = new AtomicInteger(0)
            val scoped: Int < (Scope & Abort[Throwable] & Async) =
                Scope.acquireRelease(Sync.defer(1))((_: Int) =>
                    Sync.defer { released.incrementAndGet(); () }.andThen(Abort.fail(releaseBoom))
                )
            KyoToCats.runResource[CatsIO, Int](scoped).use(v => CatsIO(assert(v == 1))).attempt.flatMap {
                case Right(_) => CatsIO(assert(released.get() == 1, "release must have run"))
                case Left(ex) => CatsIO(fail(s"expected use to succeed despite release failure, got $ex"))
            }
        }

        "async release is drained before use returns" in runCatsIO {
            val released                                         = new AtomicInteger(0)
            val scoped: Int < (Scope & Abort[Throwable] & Async) =
                Scope.acquireRelease(Sync.defer(5))((_: Int) => Async.delay(20.millis)(released.incrementAndGet()).map(_ => ()))
            KyoToCats.runResource[CatsIO, Int](scoped).use(v => CatsIO(assert(v == 5))) >>
                CatsIO(assert(released.get() == 1, "async release was not awaited"))
        }

        "releases run LIFO" in runCatsIO {
            val counter                                           = new AtomicInteger(0)
            val order1                                            = new AtomicInteger(-1)
            val order2                                            = new AtomicInteger(-1)
            def scoped1: Int < (Scope & Abort[Throwable] & Async) =
                Scope.acquireRelease(Sync.defer(1))((_: Int) => Sync.defer { order1.set(counter.incrementAndGet()); () })
            def scoped2: Int < (Scope & Abort[Throwable] & Async) =
                Scope.acquireRelease(Sync.defer(2))((_: Int) => Sync.defer { order2.set(counter.incrementAndGet()); () })
            val combined: (Int, Int) < (Scope & Abort[Throwable] & Async) =
                for
                    a <- scoped1
                    b <- scoped2
                yield (a, b)
            KyoToCats.runResource[CatsIO, (Int, Int)](combined).use { case (a, b) =>
                CatsIO(assert(a == 1 && b == 2))
            } >> CatsIO {
                assert(counter.get() == 2, "each release must run exactly once")
                assert(order2.get() == 1 && order1.get() == 2, s"expected LIFO, got order1=${order1.get()} order2=${order2.get()}")
            }
        }

        "resource is reusable across uses" in runCatsIO {
            val acquired                                         = new AtomicInteger(0)
            val released                                         = new AtomicInteger(0)
            val scoped: Int < (Scope & Abort[Throwable] & Async) =
                Scope.acquireRelease(Sync.defer { acquired.incrementAndGet(); 42 })((_: Int) =>
                    Sync.defer { released.incrementAndGet(); () }
                )
            val res = KyoToCats.runResource[CatsIO, Int](scoped)
            res.use(v => CatsIO(assert(v == 42))) >>
                res.use(v => CatsIO(assert(v == 42))) >>
                CatsIO {
                    assert(acquired.get() == 2, "each use must re-acquire")
                    assert(released.get() == 2, "each use must release")
                }
        }

        "converted resource composes with Cats DSL" in runCatsIO {
            val released                                         = new AtomicInteger(0)
            val finalized                                        = new AtomicInteger(0)
            val scoped: Int < (Scope & Abort[Throwable] & Async) =
                Scope.acquireRelease(Sync.defer(41))((_: Int) => Sync.defer { released.incrementAndGet(); () })
            val mapped        = KyoToCats.runResource[CatsIO, Int](scoped).map(_ + 1)
            val withFinalizer = mapped.onFinalize(CatsIO { finalized.incrementAndGet(); () })
            withFinalizer.use(v => CatsIO(assert(v == 42))) >> CatsIO {
                assert(released.get() == 1, "Kyo release must run")
                assert(finalized.get() == 1, "Cats onFinalize must run")
            }
        }

        "Cats flatMap nesting releases both" in runCatsIO {
            val released                                                 = new AtomicInteger(0)
            def scoped(i: Int): Int < (Scope & Abort[Throwable] & Async) =
                Scope.acquireRelease(Sync.defer(i))((_: Int) => Sync.defer { released.incrementAndGet(); () })
            val combined: CatsResource[CatsIO, Int] =
                for
                    a <- KyoToCats.runResource[CatsIO, Int](scoped(1))
                    b <- KyoToCats.runResource[CatsIO, Int](scoped(2))
                yield a + b
            combined.use(v => CatsIO(assert(v == 3))) >>
                CatsIO(assert(released.get() == 2, "both nested resources must release"))
        }

        if Platform.isJVM then
            "canceling Cats fiber during use releases" in runCatsIO {
                val started                                           = new CountDownLatch(1)
                val released                                          = new CountDownLatch(1)
                val scoped: Unit < (Scope & Abort[Throwable] & Async) =
                    Scope.acquireRelease(Sync.defer(started.countDown()))(_ => Sync.defer(released.countDown())).andThen(Async.never[Unit])
                val res = KyoToCats.runResource[CatsIO, Unit](scoped)
                for
                    f <- res.use(_ => CatsIO.never).start
                    _ <- CatsIO(assert(started.await(5, TimeUnit.SECONDS), "acquire did not start"))
                    _ <- f.cancel
                    r <- f.join
                    _ <- CatsIO(assert(released.await(5, TimeUnit.SECONDS), "release did not run"))
                yield assert(r.isCanceled)
                end for
            }

            "canceling during acquire releases prior scopes" in runCatsIO {
                val acquired                                          = new CountDownLatch(1)
                val released                                          = new CountDownLatch(1)
                val scoped: Unit < (Scope & Abort[Throwable] & Async) =
                    for
                        _ <- Scope.acquireRelease(Sync.defer(acquired.countDown()))(_ => Sync.defer(released.countDown()))
                        _ <- Async.never[Unit]
                    yield ()
                val res = KyoToCats.runResource[CatsIO, Unit](scoped)
                for
                    f <- res.use(_ => CatsIO.unit).start
                    _ <- CatsIO(assert(acquired.await(5, TimeUnit.SECONDS), "first acquire did not complete"))
                    _ <- f.cancel
                    r <- f.join
                    _ <- CatsIO(assert(released.await(5, TimeUnit.SECONDS), "prior scope was not released on acquire-cancel"))
                yield assert(r.isCanceled)
                end for
            }
        end if
    }

end KyoToCatsTest
