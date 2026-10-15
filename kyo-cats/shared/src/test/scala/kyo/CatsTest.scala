package kyo

import cats.effect.IO as CatsIO
import cats.effect.unsafe.implicits.global
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kyo.*
import kyo.internal.Platform

class CatsTest extends kyo.test.Test[Any]:

    // Run the body through the Cats Effect runtime, then bridge the resulting Future back into Kyo.
    // See https://typelevel.org/cats-effect/api/3.x/cats/effect/IO.html
    def runCatsIO[T](v: CatsIO[T]): T < Async =
        Async.fromFuture(v.unsafeToFuture())

    def runKyo(v: => Unit < (Abort[Nothing] & Async)): Unit < Async =
        Async.fromFuture(Cats.run(v).unsafeToFuture())

    "Abort ordering" - {
        "kyo then cats" in runKyo {
            object catsFailure extends RuntimeException
            object kyoFailure  extends RuntimeException
            val a = Abort.fail(kyoFailure)
            val b = Cats.get(CatsIO.raiseError(catsFailure))
            Abort.run[Throwable](a.map(_ => b)).map {
                case Result.Failure(ex) =>
                    assert(ex == kyoFailure)
                case _ =>
                    fail()
            }
        }
        "cats then kyo" in runKyo {
            object catsFailure extends RuntimeException
            object kyoFailure  extends RuntimeException
            val a = Cats.get(CatsIO.raiseError(catsFailure))
            val b = Abort.fail(kyoFailure)
            Abort.run[Throwable](a.map(_ => b)).map {
                case Result.Panic(ex) =>
                    assert(ex == catsFailure)
                case ex =>
                    fail()
            }
        }
    }

    "A < Cats" in runKyo {
        val a = Cats.get(CatsIO.pure(10))
        val b = a.map(_ * 2)
        b.map(i => assert(i == 20))
    }

    "nested" in runKyo {
        val a = Cats.get(CatsIO.pure(Cats.get(CatsIO.pure("Nested")))).flatten
        a.map(s => assert(s == "Nested"))
    }

    "fibers" - {
        "basic interop" in runKyo {
            for
                v1 <- Cats.get(CatsIO.pure(1))
                v2 <- Fiber.initUnscoped(2).map(_.get)
                v3 <- Cats.get(CatsIO.pure(3))
            yield assert(v1 == 1 && v2 == 2 && v3 == 3)
        }

        "nested Kyo in Cats" in runKyo {
            Cats.get(CatsIO.defer {
                val kyoComputation = Fiber.initUnscoped(42).map(_.get)
                Cats.run(kyoComputation)
            }).map(v => assert(v == 42))
        }

        "nested Cats in Kyo" in runKyo {
            val nestedCats = Cats.get(CatsIO.pure("nested"))
            Fiber.initUnscoped(nestedCats).map(_.get).map(s => assert(s == "nested"))
        }

        "complex nested pattern with parallel and race" in runKyo {
            def kyoTask(i: Int): Int < (Abort[Nothing] & Async)  = Fiber.initUnscoped(i * 2).map(_.get)
            def catsTask(i: Int): Int < (Abort[Nothing] & Async) = Cats.get(CatsIO.pure(i + 1))

            for
                (v1, v2) <- Async.zip(kyoTask(1), catsTask(2))
                (v3, v4) <- Async.race(
                    Async.zip[Nothing, Int, Int, Any](kyoTask(v1), catsTask(v2)),
                    Cats.get(CatsIO.pure((v1, v2)))
                )
                (v5, v6) <- Async.zip(
                    kyoTask(v3 + v4),
                    Async.race[Nothing, Int, Any](catsTask(v1), kyoTask(v2))
                )
                result <- Cats.get(CatsIO.pure(v1 + v2 + v4 + v5))
            yield assert(result >= 15 && result <= 30)
            end for
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
                        f <- Cats.run(kyoLoop(started, done)).start
                        _ <- CatsIO(started.await(100, TimeUnit.MILLISECONDS))
                        _ <- f.cancel
                        r <- f.join
                        _ <- CatsIO(done.await(100, TimeUnit.MILLISECONDS))
                    yield assert(r.isCanceled)
                    end for
                }

                "both" in runCatsIO {
                    val started = new CountDownLatch(2)
                    val done    = new CountDownLatch(2)
                    val v       =
                        for
                            _ <- Cats.get(catsLoop(started, done))
                            _ <- Fiber.initUnscoped(kyoLoop(started, done))
                        yield ()
                    for
                        f <- Cats.run(v).start
                        _ <- CatsIO(started.await(100, TimeUnit.MILLISECONDS))
                        _ <- f.cancel
                        r <- f.join
                        _ <- CatsIO(done.await(100, TimeUnit.MILLISECONDS))
                    yield assert(r.isCanceled)
                    end for
                }

                "parallel loops" in runCatsIO {
                    val started        = new CountDownLatch(2)
                    val done           = new CountDownLatch(2)
                    def parallelEffect =
                        Cats.run {
                            val loop1 = Cats.get(catsLoop(started, done))
                            val loop2 = kyoLoop(started, done)
                            Async.zip[Throwable, Unit, Unit, Any](loop1, loop2)
                        }
                    for
                        f <- parallelEffect.start
                        _ <- CatsIO(started.await(100, TimeUnit.MILLISECONDS))
                        _ <- f.cancel
                        r <- f.join
                        _ <- CatsIO(done.await(100, TimeUnit.MILLISECONDS))
                    yield assert(r.isCanceled)
                    end for
                }

                "race loops" in runCatsIO {
                    val started    = new CountDownLatch(2)
                    val done       = new CountDownLatch(2)
                    def raceEffect =
                        Cats.run {
                            val loop1 = Cats.get(catsLoop(started, done))
                            val loop2 = kyoLoop(started, done)
                            Async.race[Throwable, Unit, Any](loop1, loop2)
                        }
                    for
                        f <- raceEffect.start
                        _ <- CatsIO(started.await(100, TimeUnit.MILLISECONDS))
                        _ <- f.cancel
                        r <- f.join
                        _ <- CatsIO(done.await(100, TimeUnit.MILLISECONDS))
                    yield assert(r.isCanceled)
                    end for
                }
            }

            "runKyo" - {
                "kyo to cats" in runKyo {
                    val started = new CountDownLatch(1)
                    val done    = new CountDownLatch(1)
                    val panic   = Result.Panic(new Exception)
                    for
                        f <- Fiber.initUnscoped(Cats.get(catsLoop(started, done)))
                        _ <- Sync.defer(started.await(100, TimeUnit.MILLISECONDS))
                        _ <- f.interrupt(panic)
                        r <- f.getResult
                        _ <- Sync.defer(done.await(100, TimeUnit.MILLISECONDS))
                    yield assert(r == panic)
                    end for
                }

                "both" in runKyo {
                    val started = new CountDownLatch(2)
                    val done    = new CountDownLatch(2)
                    val v       =
                        for
                            _ <- Cats.get(catsLoop(started, done))
                            _ <- kyoLoop(started, done)
                        yield ()
                    for
                        f <- Fiber.initUnscoped(v)
                        _ <- Sync.defer(started.await(100, TimeUnit.MILLISECONDS))
                        _ <- f.interrupt
                        r <- f.getResult
                        _ <- Sync.defer(done.await(100, TimeUnit.MILLISECONDS))
                    yield assert(r.isPanic)
                    end for
                }

                "parallel loops" in runKyo {
                    val started        = new CountDownLatch(2)
                    val done           = new CountDownLatch(2)
                    def parallelEffect =
                        val loop1 = Cats.get(catsLoop(started, done))
                        val loop2 = kyoLoop(started, done)
                        Async.zip[Throwable, Unit, Unit, Any](loop1, loop2)
                    end parallelEffect
                    for
                        f <- Fiber.initUnscoped(parallelEffect)
                        _ <- Sync.defer(started.await(100, TimeUnit.MILLISECONDS))
                        _ <- f.interrupt
                        r <- f.getResult
                        _ <- Sync.defer(done.await(100, TimeUnit.MILLISECONDS))
                    yield assert(r.isPanic)
                    end for
                }

                "race loops" in runKyo {
                    val started    = new CountDownLatch(2)
                    val done       = new CountDownLatch(2)
                    def raceEffect =
                        val loop1 = Cats.get(catsLoop(started, done))
                        val loop2 = kyoLoop(started, done)
                        Async.race(loop1, loop2)
                    end raceEffect
                    for
                        f <- Fiber.initUnscoped(raceEffect)
                        _ <- Sync.defer(started.await(100, TimeUnit.MILLISECONDS))
                        _ <- f.interrupt
                        r <- f.getResult
                        _ <- Sync.defer(done.await(100, TimeUnit.MILLISECONDS))
                    yield assert(r.isPanic)
                    end for
                }
            }
        end if
    }

    "Error handling" - {
        "Kyo Abort to Cats IO error" in runKyo {
            val kyoAbort  = Abort.fail(new Exception("Kyo error"))
            val converted = Cats.get(CatsIO.fromEither(Abort.run(kyoAbort).eval.toEither))
            Abort.run[Throwable](converted).map {
                case Result.Panic(ex) => assert(ex.getMessage() == "Kyo error")
                case _                => fail("Expected a String error")
            }
        }

        "Cats IO error to Kyo Abort" in runKyo {
            val catsError = CatsIO.raiseError[Int](new Exception("Cats error"))
            val converted = Cats.get(catsError)
            Abort.run[Throwable](converted).map {
                case Result.Panic(error: Exception) => assert(error.getMessage == "Cats error")
                case _                              => fail("Expected an Exception")
            }
        }
    }

    "F-generic bridges" - {
        "lift via Dispatcher" in runCatsIO {
            import cats.effect.std.Dispatcher as CatsDispatcher
            CatsDispatcher.parallel[CatsIO].use { disp =>
                given CatsDispatcher[CatsIO]               = disp
                val lifted: Int < (Abort[Nothing] & Async) = Cats.lift(CatsIO.pure(7))
                Cats.run(Abort.run[Throwable](lifted).map {
                    case Result.Success(v) => assert(v == 7)
                    case other             => throw new AssertionError(s"unexpected $other")
                })
            }
        }

        "runF generic to IO" in runCatsIO {
            val kyoWork: Int < (Abort[Throwable] & Async) = Async.delay(1.millis)(42)
            Cats.runF[CatsIO, Int](kyoWork).map(v => assert(v == 42))
        }

        "runF generic to EitherT" in runCatsIO {
            import cats.data.EitherT
            type Eith[A] = EitherT[CatsIO, Throwable, A]
            val kyoWork: Int < (Abort[Throwable] & Async) = 99
            val eff: Eith[Int]                            =
                Cats.runF[Eith, Int](kyoWork)
            eff.value.flatMap {
                case Right(v) => CatsIO(assert(v == 99))
                case Left(e)  => CatsIO.raiseError(e)
            }
        }

        "lift failure becomes Panic" in runCatsIO {
            import cats.effect.std.Dispatcher as CatsDispatcher
            CatsDispatcher.parallel[CatsIO].use { disp =>
                given CatsDispatcher[CatsIO]               = disp
                val boom                                   = new RuntimeException("lift boom")
                val lifted: Int < (Abort[Nothing] & Async) = Cats.lift(CatsIO.raiseError[Int](boom))
                Cats.run(Abort.run[Throwable](lifted).map {
                    case Result.Panic(ex) => assert(ex == boom)
                    case other            => fail(s"expected Panic, got $other")
                })
            }
        }

        "runF maps Abort.fail to F error" in runCatsIO {
            val boom                                      = new RuntimeException("kyo abort")
            val kyoWork: Int < (Abort[Throwable] & Async) = Abort.fail(boom)
            Cats.runF[CatsIO, Int](kyoWork).attempt.map {
                case Left(ex) => assert(ex == boom)
                case Right(v) => fail(s"expected error, got $v")
            }
        }

        "runF maps Kyo panic to F error" in runCatsIO {
            val boom                                       = new RuntimeException("kyo panic")
            val kyoPanic: Int < (Abort[Throwable] & Async) = Cats.get(CatsIO.raiseError[Int](boom))
            Cats.runF[CatsIO, Int](kyoPanic).attempt.map {
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
                            Cats.run {
                                val looped = Cats.lift(CatsIO(started.countDown()) >> CatsIO.never.guarantee(CatsIO(done.countDown())))
                                Async.zip[Throwable, Unit, Unit, Any](looped, Async.never[Unit])
                            }
                        for
                            f <- effect.start
                            _ <- CatsIO(started.await(100, TimeUnit.MILLISECONDS))
                            _ <- f.cancel
                            r <- f.join
                            _ <- CatsIO(done.await(100, TimeUnit.MILLISECONDS))
                        yield assert(r.isCanceled)
                        end for
                    }
                }

                "interrupting Kyo cancels lifted F" in runKyo {
                    import cats.effect.std.Dispatcher as CatsDispatcher
                    val started                = new CountDownLatch(1)
                    val done                   = new CountDownLatch(1)
                    def catsWork: CatsIO[Unit] =
                        CatsIO(started.countDown()) >> CatsIO.never.guarantee(CatsIO(done.countDown()))
                    val panic = Result.Panic(new Exception)
                    for
                        alloc <- Cats.get(CatsDispatcher.parallel[CatsIO].allocated)
                        r     <-
                            given CatsDispatcher[CatsIO] = alloc._1
                            for
                                f <- Fiber.initUnscoped(Cats.lift(catsWork))
                                _ <- Sync.defer(started.await(100, TimeUnit.MILLISECONDS))
                                _ <- f.interrupt(panic)
                                r <- f.getResult
                                _ <- Sync.defer(done.await(100, TimeUnit.MILLISECONDS))
                            yield r
                            end for
                        _ <- Cats.get(alloc._2)
                    yield assert(r == panic)
                    end for
                }
            }
        end if
    }

    "minimal typeclasses" - {
        import kyo.Cats.given

        def runKyoT(v: => Unit < (Abort[Throwable] & Async)): Unit < Async =
            Async.fromFuture(Cats.run(v).unsafeToFuture())

        "Async summon resolves Sync/Temporal bounds" in runKyoT {
            def needsSync[F[_]](using cats.effect.kernel.Sync[F]): F[Int]   = summon[cats.effect.kernel.Sync[F]].pure(3)
            def needsAsync[F[_]](using cats.effect.kernel.Async[F]): F[Int] = summon[cats.effect.kernel.Async[F]].pure(4)
            for
                a <- needsSync[Cats.Suspend]
                b <- needsAsync[Cats.Suspend]
            yield assert(a == 3 && b == 4)
            end for
        }

        "MonadError recovers Failure and Panic" in runKyoT {
            val F                                             = summon[cats.effect.kernel.Async[Cats.Suspend]]
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
            val F                            = summon[cats.effect.kernel.Async[Cats.Suspend]]
            val panicking: Cats.Suspend[Int] = Cats.get(CatsIO.raiseError[Int](new RuntimeException("boom")))
            Abort.run[Throwable](F.handleErrorWith(panicking)(_ => 7)).map(r => assert(r == Result.succeed(7)))
        }

        "start and join through instance" in runKyoT {
            val F = summon[cats.effect.kernel.Async[Cats.Suspend]]
            for
                fib <- F.start(F.pure(42))
                v   <- fib.joinWithNever
            yield assert(v == 42)
            end for
        }

        "async roundtrip through instance" in runKyoT {
            val F = summon[cats.effect.kernel.Async[Cats.Suspend]]
            F.async_[Int](cb => cb(Right(5))).map(v => assert(v == 5))
        }

        "Ref/Deferred roundtrip through instance" in runKyoT {
            val F = summon[cats.effect.kernel.Async[Cats.Suspend]]
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
            val F = summon[cats.effect.kernel.Async[Cats.Suspend]]
            for
                _ <- F.sleep(scala.concurrent.duration.FiniteDuration(1, "ms"))
                _ <- F.uncancelable(_ => 5)
                _ <- F.onCancel(6, F.unit)
            yield assert(true)
            end for
        }
    }

end CatsTest
