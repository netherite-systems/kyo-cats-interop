package kyo

import cats.effect.IO as CatsIO
import cats.effect.kernel.Resource as CatsResource
import cats.effect.std.Dispatcher as CatsDispatcher
import cats.effect.unsafe.implicits.global
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kyo.*
import kyo.internal.Platform

class CatsToKyoTest extends kyo.test.Test[Any]:

    // Run the body through the Cats Effect runtime, then bridge the resulting Future back into Kyo.
    // See https://typelevel.org/cats-effect/api/3.x/cats/effect/IO.html
    def runCatsIO[T](v: CatsIO[T]): T < Async =
        Async.fromFuture(v.unsafeToFuture())

    def runKyo(v: => Unit < (Abort[Nothing] & Async)): Unit < Async =
        Async.fromFuture(KyoToCats.run(v).unsafeToFuture())

    "Abort ordering" - {
        "kyo then cats" in runKyo {
            object catsFailure extends RuntimeException
            object kyoFailure  extends RuntimeException
            val a = Abort.fail(kyoFailure)
            val b = CatsToKyo.get(CatsIO.raiseError(catsFailure))
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
            val a = CatsToKyo.get(CatsIO.raiseError(catsFailure))
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
        val a = CatsToKyo.get(CatsIO.pure(10))
        val b = a.map(_ * 2)
        b.map(i => assert(i == 20))
    }

    "nested" in runKyo {
        val a = CatsToKyo.get(CatsIO.pure(CatsToKyo.get(CatsIO.pure("Nested")))).flatten
        a.map(s => assert(s == "Nested"))
    }

    "fibers" - {
        "basic interop" in runKyo {
            for
                v1 <- CatsToKyo.get(CatsIO.pure(1))
                v2 <- Fiber.initUnscoped(2).map(_.get)
                v3 <- CatsToKyo.get(CatsIO.pure(3))
            yield assert(v1 == 1 && v2 == 2 && v3 == 3)
        }

        "nested Cats in Kyo" in runKyo {
            val nestedCats = CatsToKyo.get(CatsIO.pure("nested"))
            Fiber.initUnscoped(nestedCats).map(_.get).map(s => assert(s == "nested"))
        }

        "complex nested pattern with parallel and race" in runKyo {
            def kyoTask(i: Int): Int < (Abort[Nothing] & Async)  = Fiber.initUnscoped(i * 2).map(_.get)
            def catsTask(i: Int): Int < (Abort[Nothing] & Async) = CatsToKyo.get(CatsIO.pure(i + 1))

            for
                (v1, v2) <- Async.zip(kyoTask(1), catsTask(2))
                (v3, v4) <- Async.race(
                    Async.zip[Nothing, Int, Int, Any](kyoTask(v1), catsTask(v2)),
                    CatsToKyo.get(CatsIO.pure((v1, v2)))
                )
                (v5, v6) <- Async.zip(
                    kyoTask(v3 + v4),
                    Async.race[Nothing, Int, Any](catsTask(v1), kyoTask(v2))
                )
                result <- CatsToKyo.get(CatsIO.pure(v1 + v2 + v4 + v5))
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

            "runKyo" - {
                "kyo to cats" in runKyo {
                    val started = new CountDownLatch(1)
                    val done    = new CountDownLatch(1)
                    val panic   = Result.Panic(new Exception)
                    for
                        f <- Fiber.initUnscoped(CatsToKyo.get(catsLoop(started, done)))
                        _ <- Sync.defer(assert(started.await(5, TimeUnit.SECONDS), "loop did not start"))
                        _ <- f.interrupt(panic)
                        r <- f.getResult
                        _ <- Sync.defer(assert(done.await(5, TimeUnit.SECONDS), "finalizer did not run"))
                    yield assert(r == panic)
                    end for
                }

                "both" in runKyo {
                    val started = new CountDownLatch(2)
                    val done    = new CountDownLatch(2)
                    val v       =
                        Async.zip[Throwable, Unit, Unit, Any](
                            CatsToKyo.get(catsLoop(started, done)),
                            kyoLoop(started, done)
                        )
                    for
                        f <- Fiber.initUnscoped(v)
                        _ <- Sync.defer(assert(started.await(5, TimeUnit.SECONDS), "loops did not start"))
                        _ <- f.interrupt
                        r <- f.getResult
                        _ <- Sync.defer(assert(done.await(5, TimeUnit.SECONDS), "finalizers did not run"))
                    yield assert(r.isPanic)
                    end for
                }

                "parallel loops" in runKyo {
                    val started        = new CountDownLatch(2)
                    val done           = new CountDownLatch(2)
                    def parallelEffect =
                        val loop1 = CatsToKyo.get(catsLoop(started, done))
                        val loop2 = kyoLoop(started, done)
                        Async.zip[Throwable, Unit, Unit, Any](loop1, loop2)
                    end parallelEffect
                    for
                        f <- Fiber.initUnscoped(parallelEffect)
                        _ <- Sync.defer(assert(started.await(5, TimeUnit.SECONDS), "loops did not start"))
                        _ <- f.interrupt
                        r <- f.getResult
                        _ <- Sync.defer(assert(done.await(5, TimeUnit.SECONDS), "finalizers did not run"))
                    yield assert(r.isPanic)
                    end for
                }

                "race loops" in runKyo {
                    val started    = new CountDownLatch(2)
                    val done       = new CountDownLatch(2)
                    def raceEffect =
                        val loop1 = CatsToKyo.get(catsLoop(started, done))
                        val loop2 = kyoLoop(started, done)
                        Async.race(loop1, loop2)
                    end raceEffect
                    for
                        f <- Fiber.initUnscoped(raceEffect)
                        _ <- Sync.defer(assert(started.await(5, TimeUnit.SECONDS), "loops did not start"))
                        _ <- f.interrupt
                        r <- f.getResult
                        _ <- Sync.defer(assert(done.await(5, TimeUnit.SECONDS), "finalizers did not run"))
                    yield assert(r.isPanic)
                    end for
                }
            }
        end if
    }

    "Error handling" - {
        "Kyo Abort to Cats IO error" in runKyo {
            val kyoAbort  = Abort.fail(new Exception("Kyo error"))
            val converted = CatsToKyo.get(CatsIO.fromEither(Abort.run(kyoAbort).eval.toEither))
            Abort.run[Throwable](converted).map {
                case Result.Panic(ex) => assert(ex.getMessage() == "Kyo error")
                case _                => fail("Expected a String error")
            }
        }

        "Cats IO error to Kyo Abort" in runKyo {
            val catsError = CatsIO.raiseError[Int](new Exception("Cats error"))
            val converted = CatsToKyo.get(catsError)
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
                val lifted: Int < (Abort[Nothing] & Async) = CatsToKyo.lift(CatsIO.pure(7))
                KyoToCats.run(Abort.run[Throwable](lifted).map {
                    case Result.Success(v) => assert(v == 7)
                    case other             => throw new AssertionError(s"unexpected $other")
                })
            }
        }

        "lift failure becomes Panic" in runCatsIO {
            import cats.effect.std.Dispatcher as CatsDispatcher
            CatsDispatcher.parallel[CatsIO].use { disp =>
                given CatsDispatcher[CatsIO]               = disp
                val boom                                   = new RuntimeException("lift boom")
                val lifted: Int < (Abort[Nothing] & Async) = CatsToKyo.lift(CatsIO.raiseError[Int](boom))
                KyoToCats.run(Abort.run[Throwable](lifted).map {
                    case Result.Panic(ex) => assert(ex == boom)
                    case other            => fail(s"expected Panic, got $other")
                })
            }
        }

        if Platform.isJVM then
            "cancellation across the generic boundary" - {
                "interrupting Kyo cancels lifted F" in runKyo {
                    import cats.effect.std.Dispatcher as CatsDispatcher
                    val started                = new CountDownLatch(1)
                    val done                   = new CountDownLatch(1)
                    def catsWork: CatsIO[Unit] =
                        CatsIO(started.countDown()) >> CatsIO.never.guarantee(CatsIO(done.countDown()))
                    val panic = Result.Panic(new Exception)
                    for
                        alloc <- CatsToKyo.get(CatsDispatcher.parallel[CatsIO].allocated)
                        r     <-
                            given CatsDispatcher[CatsIO] = alloc._1
                            for
                                f <- Fiber.initUnscoped(CatsToKyo.lift(catsWork))
                                _ <- Sync.defer(assert(started.await(5, TimeUnit.SECONDS), "lifted effect did not start"))
                                _ <- f.interrupt(panic)
                                r <- f.getResult
                                _ <- Sync.defer(assert(done.await(5, TimeUnit.SECONDS), "lifted finalizer did not run"))
                            yield r
                            end for
                        _ <- CatsToKyo.get(alloc._2)
                    yield assert(r == panic)
                    end for
                }
            }
        end if
    }

    "Resource bridges" - {
        def withDispatcher[A](f: CatsDispatcher[CatsIO] => CatsIO[A]): CatsIO[A] =
            CatsDispatcher.parallel[CatsIO].use(disp => f(disp))

        "acquire and release via Scope.run" in runCatsIO {
            withDispatcher { disp =>
                given CatsDispatcher[CatsIO]            = disp
                val acquired                            = new AtomicInteger(0)
                val released                            = new AtomicInteger(0)
                val resource: CatsResource[CatsIO, Int] =
                    CatsResource.make(CatsIO { acquired.incrementAndGet(); 42 })(_ => CatsIO { released.incrementAndGet(); () })
                KyoToCats.run {
                    Scope.run {
                        CatsToKyo.liftResource(resource).map(v => assert(v == 42))
                    }.map(_ => assert(acquired.get() == 1 && released.get() == 1))
                }
            }
        }

        "multiple resources release" in runCatsIO {
            withDispatcher { disp =>
                given CatsDispatcher[CatsIO]               = disp
                val released                               = new AtomicInteger(0)
                def res(i: Int): CatsResource[CatsIO, Int] =
                    CatsResource.make(CatsIO.pure(i))(_ => CatsIO { released.incrementAndGet(); () })
                KyoToCats.run {
                    Scope.run {
                        for
                            a <- CatsToKyo.liftResource(res(1))
                            b <- CatsToKyo.liftResource(res(2))
                        yield assert(a == 1 && b == 2)
                    }.map(_ => assert(released.get() == 2))
                }
            }
        }

        "nested resources" in runCatsIO {
            withDispatcher { disp =>
                given CatsDispatcher[CatsIO]            = disp
                val inner: CatsResource[CatsIO, String] =
                    CatsResource.make(CatsIO.pure("Nested"))(_ => CatsIO.unit)
                val outer: CatsResource[CatsIO, String] = inner.map(_ + "!")
                KyoToCats.run {
                    Scope.run(CatsToKyo.liftResource(outer).map(s => assert(s == "Nested!")))
                }
            }
        }

        "allocate failure becomes Panic" in runCatsIO {
            withDispatcher { disp =>
                given CatsDispatcher[CatsIO] = disp
                val boom                     = new RuntimeException("resource boom")
                val resource                 = CatsResource.raiseError[CatsIO, Int, Throwable](boom)
                KyoToCats.run(Abort.run[Throwable](Scope.run(CatsToKyo.liftResource(resource))).map {
                    case Result.Panic(ex) => assert(ex == boom)
                    case other            => fail(s"expected Panic, got $other")
                })
            }
        }

        "abort ordering with resource" - {
            "kyo failure before resource wins" in runCatsIO {
                withDispatcher { disp =>
                    given CatsDispatcher[CatsIO] = disp
                    object catsFailure extends RuntimeException
                    object kyoFailure  extends RuntimeException
                    val resource: CatsResource[CatsIO, Int] =
                        CatsResource.make(CatsIO.raiseError[Int](catsFailure))(_ => CatsIO.unit)
                    KyoToCats.run(Abort.run[Throwable] {
                        Scope.run(Abort.fail(kyoFailure).map(_ => CatsToKyo.liftResource(resource)))
                    }.map {
                        case Result.Failure(ex) => assert(ex == kyoFailure)
                        case other              => fail(s"expected Failure, got $other")
                    })
                }
            }

            "resource failure before kyo wins as Panic" in runCatsIO {
                withDispatcher { disp =>
                    given CatsDispatcher[CatsIO] = disp
                    object catsFailure extends RuntimeException
                    object kyoFailure  extends RuntimeException
                    val resource: CatsResource[CatsIO, Int] =
                        CatsResource.make(CatsIO.raiseError[Int](catsFailure))(_ => CatsIO.unit)
                    KyoToCats.run(Abort.run[Throwable] {
                        Scope.run(CatsToKyo.liftResource(resource).map(_ => Abort.fail(kyoFailure))).flatten
                    }.map {
                        case Result.Panic(ex) => assert(ex == catsFailure)
                        case other            => fail(s"expected Panic, got $other")
                    })
                }
            }
        }

        "resource composes with fibers" in runCatsIO {
            withDispatcher { disp =>
                given CatsDispatcher[CatsIO]               = disp
                def res(i: Int): CatsResource[CatsIO, Int] =
                    CatsResource.make(CatsIO.pure(i))(_ => CatsIO.unit)
                KyoToCats.run {
                    Scope.run {
                        for
                            v1 <- CatsToKyo.liftResource(res(1))
                            v2 <- Fiber.initUnscoped(2).map(_.get)
                            v3 <- CatsToKyo.liftResource(res(3))
                        yield assert(v1 == 1 && v2 == 2 && v3 == 3)
                    }
                }
            }
        }

        "partial acquire failure releases first resource" in runCatsIO {
            withDispatcher { disp =>
                given CatsDispatcher[CatsIO]         = disp
                val releasedOk                       = new AtomicInteger(0)
                val releasedFail                     = new AtomicInteger(0)
                val boom                             = new RuntimeException("second Cats acquire boom")
                def okRes: CatsResource[CatsIO, Int] =
                    CatsResource.make(CatsIO.pure(1))(_ => CatsIO { releasedOk.incrementAndGet(); () })
                def failRes: CatsResource[CatsIO, Int] =
                    CatsResource.make(CatsIO.raiseError[Int](boom))(_ => CatsIO { releasedFail.incrementAndGet(); () })
                KyoToCats.run(Abort.run[Throwable] {
                    Scope.run {
                        for
                            _ <- CatsToKyo.liftResource(okRes)
                            _ <- CatsToKyo.liftResource(failRes)
                        yield ()
                    }
                }.map {
                    case Result.Panic(ex) => assert(ex == boom)
                    case other            => fail(s"expected Panic, got $other")
                }.map(_ =>
                    assert(releasedOk.get() == 1, "first Cats release must run")
                    assert(releasedFail.get() == 0, "failed acquire must not release")
                ))
            }
        }

        "Kyo Abort after acquire still releases Cats resource" in runCatsIO {
            withDispatcher { disp =>
                given CatsDispatcher[CatsIO]         = disp
                val released                         = new AtomicInteger(0)
                val kyoBoom                          = new RuntimeException("kyo after acquire")
                def okRes: CatsResource[CatsIO, Int] =
                    CatsResource.make(CatsIO.pure(1))(_ => CatsIO { released.incrementAndGet(); () })
                KyoToCats.run(Abort.run[Throwable] {
                    Scope.run {
                        for
                            v <- CatsToKyo.liftResource(okRes)
                            _ <- Abort.fail[Throwable](kyoBoom)
                        yield v
                    }
                }.map {
                    case Result.Failure(ex) => assert(ex == kyoBoom)
                    case other              => fail(s"expected Failure, got $other")
                }.map(_ => assert(released.get() == 1, "Cats release must run on Kyo Abort")))
            }
        }

        "Cats panic after acquire still releases prior Cats resource" in runCatsIO {
            withDispatcher { disp =>
                given CatsDispatcher[CatsIO]         = disp
                val released                         = new AtomicInteger(0)
                val catsBoom                         = new RuntimeException("cats after acquire")
                def okRes: CatsResource[CatsIO, Int] =
                    CatsResource.make(CatsIO.pure(1))(_ => CatsIO { released.incrementAndGet(); () })
                KyoToCats.run(Abort.run[Throwable] {
                    Scope.run {
                        for
                            _ <- CatsToKyo.liftResource(okRes)
                            _ <- CatsToKyo.get(CatsIO.raiseError[Unit](catsBoom))
                        yield ()
                    }
                }.map {
                    case Result.Panic(ex) => assert(ex == catsBoom)
                    case other            => fail(s"expected Panic, got $other")
                }.map(_ => assert(released.get() == 1, "prior Cats release must run")))
            }
        }

        // Kyo logs Scope finalizer failures ("Scope finalizer failed") instead of failing
        // `Scope.run`, so a failing Cats release still lets the scope succeed. This pins that behavior.
        "Cats release failure does not fail Scope.run" in runCatsIO {
            withDispatcher { disp =>
                given CatsDispatcher[CatsIO]            = disp
                val releaseBoom                         = new RuntimeException("cats release boom")
                val released                            = new AtomicInteger(0)
                val resource: CatsResource[CatsIO, Int] =
                    CatsResource.make(CatsIO.pure(1))(_ =>
                        CatsIO { released.incrementAndGet(); () } >> CatsIO.raiseError[Unit](releaseBoom)
                    )
                KyoToCats.run(Abort.run[Throwable](Scope.run(CatsToKyo.liftResource(resource))).map {
                    case Result.Success(v) => assert(v == 1)
                    case other             => fail(s"expected Success despite release failure, got $other")
                }.map(_ => assert(released.get() == 1, "Cats release must have run")))
            }
        }

        "resource construction is deferred until run" in runCatsIO {
            withDispatcher { disp =>
                given CatsDispatcher[CatsIO]       = disp
                val constructed                    = new AtomicInteger(0)
                def res: CatsResource[CatsIO, Int] =
                    CatsResource.make(CatsIO { constructed.incrementAndGet(); 1 })(_ => CatsIO.unit)
                val scoped =
                    Scope.run(CatsToKyo.liftResource(res).map(v => assert(v == 1)))
                KyoToCats.run(
                    Sync.defer(assert(constructed.get() == 0, "resource must not allocate before run"))
                        .andThen(scoped)
                        .map(_ => assert(constructed.get() == 1, "resource must allocate on run"))
                )
            }
        }

        "lifted releases run LIFO" in runCatsIO {
            withDispatcher { disp =>
                given CatsDispatcher[CatsIO]        = disp
                val counter                         = new AtomicInteger(0)
                val order1                          = new AtomicInteger(-1)
                val order2                          = new AtomicInteger(-1)
                def res1: CatsResource[CatsIO, Int] =
                    CatsResource.make(CatsIO.pure(1))(_ => CatsIO { order1.set(counter.incrementAndGet()); () })
                def res2: CatsResource[CatsIO, Int] =
                    CatsResource.make(CatsIO.pure(2))(_ => CatsIO { order2.set(counter.incrementAndGet()); () })
                KyoToCats.run {
                    Scope.run {
                        for
                            a <- CatsToKyo.liftResource(res1)
                            b <- CatsToKyo.liftResource(res2)
                        yield assert(a == 1 && b == 2)
                    }.map(_ =>
                        assert(counter.get() == 2, "each release must run exactly once")
                        assert(order2.get() == 1 && order1.get() == 2, s"expected LIFO, got ${order1.get()},${order2.get()}")
                    )
                }
            }
        }

        "same lifted value reused twice allocates twice" in runCatsIO {
            withDispatcher { disp =>
                given CatsDispatcher[CatsIO]       = disp
                val acquired                       = new AtomicInteger(0)
                val released                       = new AtomicInteger(0)
                def res: CatsResource[CatsIO, Int] =
                    CatsResource.make(CatsIO { acquired.incrementAndGet(); 7 })(_ => CatsIO { released.incrementAndGet(); () })
                KyoToCats.run {
                    Scope.run {
                        val lifted = CatsToKyo.liftResource(res)
                        for
                            a <- lifted
                            b <- lifted
                        yield assert(a == 7 && b == 7)
                        end for
                    }.map(_ =>
                        assert(acquired.get() == 2, "each bind must re-acquire")
                        assert(released.get() == 2, "each bind must release")
                    )
                }
            }
        }

        "liftResource generic to EitherT" in runCatsIO {
            import cats.data.EitherT
            type Eith[A] = EitherT[CatsIO, Throwable, A]
            CatsDispatcher.parallel[Eith].use { disp =>
                given CatsDispatcher[Eith]            = disp
                val released                          = new AtomicInteger(0)
                val resource: CatsResource[Eith, Int] =
                    CatsResource.make(EitherT.liftF(CatsIO.pure(21)))(_ => EitherT.liftF(CatsIO { released.incrementAndGet(); () }))
                EitherT.liftF(KyoToCats.run {
                    Scope.run(CatsToKyo.liftResource(resource).map(v => assert(v == 21)))
                }.map(_ => assert(released.get() == 1, "EitherT release must run")))
            }.value.flatMap {
                case Right(_) => CatsIO.unit
                case Left(e)  => CatsIO.raiseError(e)
            }
        }

        if Platform.isJVM then
            // Interrupting during *use* (acquire already complete). Interrupting a promise-joining acquire itself
            // hits Kyo's documented Scope.acquireRelease gap, so that shape is not covered here.
            "interrupting Kyo during use releases" in runKyo {
                val started                        = new CountDownLatch(1)
                val released                       = new CountDownLatch(1)
                def res: CatsResource[CatsIO, Int] =
                    CatsResource.make(CatsIO { started.countDown(); 1 })(_ => CatsIO { released.countDown(); () })
                val panic = Result.Panic(new Exception)
                for
                    alloc <- CatsToKyo.get(CatsDispatcher.parallel[CatsIO].allocated)
                    r     <-
                        given CatsDispatcher[CatsIO] = alloc._1
                        for
                            f <- Fiber.initUnscoped(Scope.run(CatsToKyo.liftResource(res).andThen(Async.never[Unit])))
                            _ <- Sync.defer(assert(started.await(5, TimeUnit.SECONDS), "resource was not acquired"))
                            _ <- f.interrupt(panic)
                            r <- f.getResult
                            _ <- Sync.defer(assert(released.await(5, TimeUnit.SECONDS), "release did not run"))
                        yield r
                        end for
                    _ <- CatsToKyo.get(alloc._2)
                yield assert(r == panic)
                end for
            }
        end if
    }

end CatsToKyoTest
