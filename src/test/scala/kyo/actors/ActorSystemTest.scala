package kyo.actors

import java.util.concurrent.atomic.AtomicReference
import kyo.{Signal as _, *}

class ActorSystemTest extends kyo.test.Test[Any]:

    override def timeout: Duration = 20.seconds

    private enum Ping derives CanEqual:
        case Boom
        case Ask(replyTo: ActorRef[Int])

    private enum Count derives CanEqual:
        case Inc
        case Boom
        case Get(replyTo: ActorRef[Int])
        case Fail
    end Count

    private enum Fifo derives CanEqual:
        case Push(n: Int)
        case Read(replyTo: ActorRef[List[Int]])

    private enum Phase derives CanEqual:
        case Switch
        case Read(replyTo: ActorRef[String])

    private def counter(n: Int): Behavior[Count] =
        Behavior.receiveMessage[Count] {
            case Count.Inc          => counter(n + 1)
            case Count.Boom         => Abort.panic(new RuntimeException("boom"))
            case Count.Fail         => Abort.fail("nope")
            case Count.Get(replyTo) => replyTo.tell(n).andThen(Behavior.same[Count])
        }

    "lifecycle" - {
        "guardian and spawned actors have hierarchical paths" in {
            ActorSystem.init("paths")(Behavior.ignore[Unit]).flatMap { system =>
                system.spawn(Behavior.ignore[String], "worker").map { ref =>
                    assert(system.name == "paths")
                    assert(system.guardian.path.render == "/user")
                    assert(ref.path.render == "/user/worker")
                    assert(ref.name == "worker")
                }
            }
        }

        "spawn rejects a duplicate name" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                Abort.run[SpawnFailure] {
                    system.spawn(Behavior.ignore[Unit], "worker").andThen {
                        system.spawn(Behavior.ignore[Unit], "worker")
                    }
                }.map { result =>
                    val duplicate = result match
                        case Result.Failure(_: ActorNameExists) => true
                        case _                                  => false
                    assert(duplicate)
                }
            }
        }

        "an empty system name panics" in {
            Abort.run[Any](ActorSystem.init("")(Behavior.ignore[Unit])).map { result =>
                val panicked = result match
                    case Result.Panic(_: InvalidBehavior) => true
                    case _                                => false
                assert(panicked)
            }
        }

        "a name containing a slash panics" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                Abort.run[Any](system.spawn(Behavior.ignore[Unit], "a/b")).map { result =>
                    assert(result.isPanic)
                }
            }
        }

        "terminate rejects later spawns" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    _      <- system.terminate
                    _      <- system.whenTerminated
                    result <- Abort.run[SpawnFailure](system.spawn(Behavior.ignore[Unit], "late"))
                yield
                    val closed = result match
                        case Result.Failure(_: ActorSystemTerminated) => true
                        case _                                        => false
                    assert(closed)
            }
        }

        "leaving the scope stops the system" in {
            for
                ref <- Scope.run {
                    ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                        system.spawn(Behavior.ignore[String], "worker")
                    }
                }
                result <- Abort.run[TellFailure](ref.tell("late"))
            yield assert(result.isFailure)
        }

        "stop rejects later tells" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    ref    <- system.spawn(Behavior.ignore[String], "worker")
                    _      <- ref.stop
                    _      <- ref.awaitTerminated
                    result <- Abort.run[TellFailure](ref.tell("late"))
                yield
                    val closed = result match
                        case Result.Failure(_: ActorClosed) => true
                        case _                              => false
                    assert(closed)
            }
        }
    }

    "messages" - {
        "tell updates state and ask reads it" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    ref   <- system.spawn(counter(0), "counter")
                    _     <- ref.tell(Count.Inc)
                    _     <- ref.tell(Count.Inc)
                    value <- ref.ask(2.seconds)(Count.Get(_))
                yield assert(value == 2)
            }
        }

        "messages are applied in order" in {
            def collect(seen: List[Int]): Behavior[Fifo] =
                Behavior.receiveMessage[Fifo] {
                    case Fifo.Push(n)       => collect(seen :+ n)
                    case Fifo.Read(replyTo) => replyTo.tell(seen).andThen(Behavior.same[Fifo])
                }
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    ref  <- system.spawn(collect(Nil), "fifo")
                    _    <- ref.tell(Fifo.Push(1))
                    _    <- ref.tell(Fifo.Push(2))
                    _    <- ref.tell(Fifo.Push(3))
                    seen <- ref.ask(2.seconds)(Fifo.Read(_))
                yield assert(seen == List(1, 2, 3))
            }
        }

        "ask times out when the actor does not reply" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    ref    <- system.spawn(Behavior.ignore[ActorRef[Int]], "quiet")
                    result <- Abort.run[AskFailure](ref.ask(30.millis)((reply: ActorRef[Int]) => reply))
                yield
                    val timedOut = result match
                        case Result.Failure(_: AskTimeout) => true
                        case _                             => false
                    assert(timedOut)
            }
        }

        "a full mailbox suspends tell until capacity frees" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    hold    <- Latch.init(1)
                    started <- Latch.init(1)
                    ref     <- system.spawn(
                        Behavior.receiveMessage[String] { _ =>
                            started.release.andThen(hold.await).andThen(Behavior.same[String])
                        },
                        "bounded",
                        Props.default.withMailbox(1)
                    )
                    _            <- ref.tell("first")
                    _            <- started.await
                    _            <- ref.tell("second")
                    blocked      <- Fiber.initUnscoped(ref.tell("third"))
                    _            <- Async.sleep(80.millis)
                    stillWaiting <- blocked.done
                    _            <- hold.release
                    _            <- blocked.get
                yield assert(!stillWaiting)
            }
        }

        "become replaces the behavior" in {
            val first: Behavior[Phase] =
                Behavior.receiveMessage[Phase] {
                    case Phase.Switch =>
                        Behavior.receiveMessage[Phase] {
                            case Phase.Read(replyTo) => replyTo.tell("second").andThen(Behavior.same[Phase])
                            case Phase.Switch        => Behavior.same[Phase]
                        }
                    case Phase.Read(_) => Behavior.same[Phase]
                }
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    ref  <- system.spawn(first, "phases")
                    _    <- ref.tell(Phase.Switch)
                    word <- ref.ask(2.seconds)(Phase.Read(_))
                yield assert(word == "second")
            }
        }
    }

    "supervision" - {
        "restart runs setup again and does not replay the failed message" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    starts <- AtomicInt.init(0)
                    behavior = Behavior.supervise {
                        Behavior.setup[Ping] { _ =>
                            starts.incrementAndGet.andThen {
                                Behavior.receiveMessage[Ping] {
                                    case Ping.Boom         => Abort.panic(new RuntimeException("boom"))
                                    case Ping.Ask(replyTo) => replyTo.tell(1).andThen(Behavior.same[Ping])
                                }
                            }
                        }
                    }.onFailure(Directive.Restart)
                    ref  <- system.spawn(behavior, "restarted")
                    _    <- ref.tell(Ping.Boom)
                    _    <- ref.ask(2.seconds)(Ping.Ask(_))
                    seen <- starts.get
                yield assert(seen == 2)
            }
        }

        "resume keeps the behavior state" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    ref   <- system.spawn(Behavior.supervise(counter(0)).onFailure(Directive.Resume), "resumed")
                    _     <- ref.tell(Count.Inc)
                    _     <- ref.tell(Count.Boom)
                    value <- ref.ask(2.seconds)(Count.Get(_))
                yield assert(value == 1)
            }
        }

        "a non-throwable abort can be resumed" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    ref   <- system.spawn(Behavior.supervise(counter(0)).onFailure(Directive.Resume), "aborted")
                    _     <- ref.tell(Count.Inc)
                    _     <- ref.tell(Count.Fail)
                    value <- ref.ask(2.seconds)(Count.Get(_))
                yield assert(value == 1)
            }
        }

        "stop ends the actor" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    ref <- system.spawn(
                        Behavior.supervise(Behavior.receiveMessage[String](_ => Abort.panic(new RuntimeException("boom"))))
                            .onFailure(Directive.Stop),
                        "stopped"
                    )
                    _ <- ref.tell("x")
                    _ <- ref.awaitTerminated
                yield assert(true)
            }
        }

        "restart limit stops the actor" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    starts <- AtomicInt.init(0)
                    behavior = Behavior.setup[String] { _ =>
                        starts.incrementAndGet.andThen {
                            Behavior.receiveMessage[String](_ => Abort.panic(new RuntimeException("boom")))
                        }
                    }
                    ref  <- system.spawn(behavior, "limited", Props.default.withSupervision(SupervisorStrategy.restart.withLimit(1)))
                    _    <- ref.tell("a")
                    _    <- ref.tell("b")
                    _    <- ref.awaitTerminated
                    seen <- starts.get
                yield assert(seen == 2)
            }
        }

        "an inner escalate is stopped by the default props strategy" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    child <- AtomicRef.init(Maybe.empty[ActorRef[String]])
                    parent = Behavior.setup[Ping] { ctx =>
                        val failing =
                            Behavior.supervise(Behavior.receiveMessage[String](_ => Abort.panic(new RuntimeException("boom"))))
                                .onFailure(Directive.Escalate)
                        ctx.spawn(failing, "child").flatMap { ref =>
                            child.set(Present(ref)).andThen {
                                Behavior.receiveMessage[Ping] {
                                    case Ping.Ask(replyTo) => replyTo.tell(1).andThen(Behavior.same[Ping])
                                    case Ping.Boom         => Behavior.same[Ping]
                                }
                            }
                        }
                    }
                    parentRef <- system.spawn(parent, "parent")
                    _         <- parentRef.ask(2.seconds)(Ping.Ask(_))
                    stored    <- child.get
                    _         <-
                        stored match
                            case Present(ref) => ref.tell("boom").andThen(ref.awaitTerminated)
                            case Absent       => Abort.panic(new RuntimeException("child missing"))
                    still <- parentRef.ask(2.seconds)(Ping.Ask(_))
                yield assert(still == 1)
            }
        }

        "escalating to the parent stops the parent under the default strategy" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    child <- AtomicRef.init(Maybe.empty[ActorRef[String]])
                    parent = Behavior.setup[Unit] { ctx =>
                        ctx.spawn(
                            Behavior.receiveMessage[String](_ => Abort.panic(new RuntimeException("boom"))),
                            "child",
                            Props.default.withSupervision(SupervisorStrategy.escalate)
                        ).flatMap { ref =>
                            child.set(Present(ref)).andThen(Behavior.ignore[Unit])
                        }
                    }
                    parentRef <- system.spawn(parent, "parent")
                    stored    <- Loop.foreach {
                        child.get.map {
                            case Present(ref) => Loop.done(ref)
                            case Absent       => Loop.continue
                        }
                    }
                    _ <- stored.tell("boom")
                    _ <- parentRef.awaitTerminated
                yield assert(true)
            }
        }

        "restart stops linked children" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    stopped <- Latch.init(1)
                    child = Behavior.ignore[Unit].onSignal { (_, signal) =>
                        signal match
                            case Signal.PostStop      => stopped.release.andThen(Behavior.stopped[Unit])
                            case _: Signal.Terminated => Behavior.same[Unit]
                    }
                    parent = Behavior.supervise {
                        Behavior.setup[Ping] { ctx =>
                            ctx.spawn(child, "kid").andThen {
                                Behavior.receiveMessage[Ping] {
                                    case Ping.Boom         => Abort.panic(new RuntimeException("boom"))
                                    case Ping.Ask(replyTo) => replyTo.tell(1).andThen(Behavior.same[Ping])
                                }
                            }
                        }
                    }.onFailure(Directive.Restart)
                    ref <- system.spawn(parent, "parent")
                    _   <- ref.tell(Ping.Boom)
                    _   <- stopped.await
                    _   <- ref.ask(2.seconds)(Ping.Ask(_))
                yield assert(true)
            }
        }
    }

    "watching" - {
        "watchWith delivers a message on permanent stop" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    seen    <- Latch.init(1)
                    watched <- system.spawn(Behavior.ignore[Unit], "watched")
                    _       <- system.spawn(
                        Behavior.setup[String] { ctx =>
                            ctx.watchWith(watched, "gone").andThen {
                                Behavior.receiveMessage[String] {
                                    case "gone" => seen.release.andThen(Behavior.stopped[String])
                                    case _      => Behavior.same[String]
                                }
                            }
                        },
                        "watcher"
                    )
                    _ <- watched.stop
                    _ <- seen.await
                yield assert(true)
            }
        }

        "restart does not emit Terminated" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    notices <- AtomicInt.init(0)
                    target  <- system.spawn(
                        Behavior.supervise {
                            Behavior.receiveMessage[Ping] {
                                case Ping.Boom         => Abort.panic(new RuntimeException("boom"))
                                case Ping.Ask(replyTo) => replyTo.tell(1).andThen(Behavior.same[Ping])
                            }
                        }.onFailure(Directive.Restart),
                        "target"
                    )
                    watcher <- system.spawn(
                        Behavior.setup[Ping] { ctx =>
                            ctx.watchWith(target, Ping.Boom).andThen {
                                Behavior.receiveMessage[Ping] {
                                    case Ping.Boom         => notices.incrementAndGet.andThen(Behavior.same[Ping])
                                    case Ping.Ask(replyTo) => replyTo.tell(1).andThen(Behavior.same[Ping])
                                }
                            }
                        },
                        "watcher"
                    )
                    _             <- target.tell(Ping.Boom)
                    _             <- target.ask(2.seconds)(Ping.Ask(_))
                    _             <- watcher.ask(2.seconds)(Ping.Ask(_))
                    duringRestart <- notices.get
                    _             <- target.stop
                    _             <- target.awaitTerminated
                    _             <- watcher.ask(2.seconds)(Ping.Ask(_))
                    afterStop     <- notices.get
                yield
                    assert(duringRestart == 0)
                    assert(afterStop == 1)
            }
        }

        "watch without a handler stops the watcher" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    watched <- system.spawn(Behavior.ignore[Unit], "watched")
                    watcher <- system.spawn(
                        Behavior.setup[Unit] { ctx =>
                            ctx.watch(watched).andThen(Behavior.ignore[Unit])
                        },
                        "watcher"
                    )
                    _ <- watched.stop
                    _ <- watcher.awaitTerminated
                yield assert(true)
            }
        }

        "unwatch stops further notices" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    notices <- AtomicInt.init(0)
                    watched <- system.spawn(Behavior.ignore[Unit], "watched")
                    watcher <- system.spawn(
                        Behavior.setup[String] { ctx =>
                            ctx.watchWith(watched, "died").andThen {
                                ctx.unwatch(watched).andThen {
                                    Behavior.receiveMessage[String] { _ =>
                                        notices.incrementAndGet.andThen(Behavior.same[String])
                                    }
                                }
                            }
                        },
                        "watcher"
                    )
                    _    <- watched.stop
                    _    <- watched.awaitTerminated
                    _    <- Async.sleep(50.millis)
                    seen <- notices.get
                    _    <- watcher.stop
                yield assert(seen == 0)
            }
        }
    }

    "children" - {
        "a parent stop stops its children" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    stopped <- Latch.init(1)
                    child = Behavior.ignore[Unit].onSignal { (_, signal) =>
                        signal match
                            case Signal.PostStop      => stopped.release.andThen(Behavior.stopped[Unit])
                            case _: Signal.Terminated => Behavior.same[Unit]
                    }
                    parent = Behavior.setup[String] { ctx =>
                        ctx.spawn(child, "kid").andThen {
                            Behavior.receiveMessage[String] {
                                case "stop" => Behavior.stopped[String]
                                case _      => Behavior.same[String]
                            }
                        }
                    }
                    ref <- system.spawn(parent, "parent")
                    _   <- ref.tell("stop")
                    _   <- stopped.await
                yield assert(true)
            }
        }

        "context.stop stops a direct child" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    stopped <- Latch.init(1)
                    child = Behavior.ignore[Unit].onSignal { (_, signal) =>
                        signal match
                            case Signal.PostStop      => stopped.release.andThen(Behavior.stopped[Unit])
                            case _: Signal.Terminated => Behavior.same[Unit]
                    }
                    parent = Behavior.setup[String] { ctx =>
                        ctx.spawn(child, "kid").flatMap { kid =>
                            Behavior.receiveMessage[String] {
                                case "stop" => ctx.stop(kid).andThen(Behavior.same[String])
                                case _      => Behavior.same[String]
                            }
                        }
                    }
                    ref <- system.spawn(parent, "parent")
                    _   <- ref.tell("stop")
                    _   <- stopped.await
                yield assert(true)
            }
        }

        "stopping a non-child panics" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                val seen     = new AtomicReference("")
                val strategy = SupervisorStrategy { cause =>
                    seen.set(cause.getMessage)
                    Directive.Stop
                }
                val behavior = Behavior.receive[Unit] { (ctx, _) =>
                    ctx.stop(system.guardian).andThen(Behavior.stopped[Unit])
                }
                for
                    ref <- system.spawn(behavior, "bad-stop", Props.default.withSupervision(strategy))
                    _   <- ref.tell(())
                    _   <- ref.awaitTerminated
                yield assert(seen.get.contains("not a child"))
                end for
            }
        }

        "nested spawn uses the parent path" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    parent <- system.spawn(
                        Behavior.setup[ActorRef[String]] { ctx =>
                            ctx.spawn(Behavior.ignore[String], "kid").flatMap { kid =>
                                Behavior.receiveMessage[ActorRef[String]] { reply =>
                                    reply.tell(kid.path.render).andThen(Behavior.same[ActorRef[String]])
                                }
                            }
                        },
                        "parent"
                    )
                    path <- parent.ask(2.seconds)((reply: ActorRef[String]) => reply)
                yield assert(path == "/user/parent/kid")
            }
        }
    }

    "receive timeout" - {
        "a quiet actor receives the timeout message" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    seen <- Latch.init(1)
                    _    <- system.spawn(
                        Behavior.setup[String] { ctx =>
                            ctx.setReceiveTimeout(40.millis, "tick").andThen {
                                Behavior.receiveMessage[String] {
                                    case "tick" => seen.release.andThen(Behavior.stopped[String])
                                    case _      => Behavior.same[String]
                                }
                            }
                        },
                        "timer"
                    )
                    _ <- seen.await
                yield assert(true)
            }
        }

        "cancelling the timeout drops it" in {
            ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
                for
                    hits <- AtomicInt.init(0)
                    _    <- system.spawn(
                        Behavior.setup[String] { ctx =>
                            ctx.setReceiveTimeout(40.millis, "tick").andThen {
                                ctx.cancelReceiveTimeout.andThen {
                                    Behavior.receiveMessage[String] { _ =>
                                        hits.incrementAndGet.andThen(Behavior.same[String])
                                    }
                                }
                            }
                        },
                        "quiet"
                    )
                    _    <- Async.sleep(120.millis)
                    seen <- hits.get
                yield assert(seen == 0)
            }
        }
    }
end ActorSystemTest
