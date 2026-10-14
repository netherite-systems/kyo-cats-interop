package kyo.actors.internal

import kyo.{Signal as _, *}
import kyo.actors.*
import scala.annotation.tailrec

/** Interpreter that runs [[Behavior]] values on `kyo.Actor`.
  *
  * Each user actor is one Kyo actor. The mailbox carries internal envelopes so user messages,
  * stop, watch notifications, and escalations stay in a single FIFO queue.
  */
private[actors] object Dispatch:

    /** Mailbox envelope. `User` is the only variant produced by [[ActorRef.tell]]. */
    enum Mail derives CanEqual:
        case User(message: Any)
        case Stop
        case Signal(signal: kyo.actors.Signal)
        case Escalated(cause: Throwable)
    end Mail

    private enum Slot derives CanEqual:
        case Pending
        case Ready(ref: ActorRef[?])
    end Slot

    final private class Registration(
        val key: AnyRef,
        cancelled: AtomicBoolean,
        gate: AtomicBoolean,
        callback: Maybe[Throwable] => Unit < Async
    ):
        def cancel(using Frame): Unit < Sync = cancelled.set(true)

        def fire(cause: Maybe[Throwable])(using Frame): Unit < Async =
            cancelled.get.flatMap { isCancelled =>
                if isCancelled then ()
                else
                    gate.compareAndSet(false, true).flatMap { won =>
                        if won then callback(cause) else ()
                    }
            }
    end Registration

    /** Outcome of one incarnation. `Continue` stays inside the receive loop. */
    private enum Outcome[M]:
        case Continue(ready: Ready[M])
        case Restart[M]()                     extends Outcome[M]
        case Halt[M](cause: Maybe[Throwable]) extends Outcome[M]
    end Outcome

    private enum Ready[M]:
        case Receive(onMessage: Behavior.Handle[M], onSignal: Maybe[Behavior.SignalHandle[M]])
        case Ignore(onSignal: Maybe[Behavior.SignalHandle[M]])

        def signals: Maybe[Behavior.SignalHandle[M]] =
            this match
                case Receive(_, onSignal) => onSignal
                case Ignore(onSignal)     => onSignal
    end Ready

    private enum Decision derives CanEqual:
        case Resume
        case Restart
        case Stop(cause: Maybe[Throwable])
        case Escalate(cause: Throwable)
    end Decision

    private type Running = Actor[Nothing, Mail, Unit]

    def system[M](name: String)(guardianBehavior: Behavior[M])(using Frame): ActorSystem[M] < (Async & Scope) =
        if name.isEmpty then Abort.panic(InvalidBehavior("actor system name must not be empty"))
        else
            for
                alive <- AtomicBoolean.init(true)
                shell = SystemShell[M](name, alive)
                guardianCell <- Cell.open[M](shell, ActorPath.user, Absent)
                wrapped = Behavior.supervise(guardianBehavior).onFailure(Props.default.supervision)
                guardian <- start(guardianCell, wrapped, Props.default.mailboxCapacity)
                _ = shell.attach(guardianCell, guardian)
                // Closing the enclosing scope stops the system, same as `Scope` cleanup for `Actor.run`.
                // https://getkyo.io/latest/kyo-actor/
                _ <- Scope.ensure(shell.terminate)
            yield shell
    end system

    private def spawnOn[N](
        parent: Cell[?],
        name: String,
        behavior: Behavior[N],
        props: Props
    )(using Frame): ActorRef[N] < (Async & Scope & Abort[SpawnFailure]) =
        if name.isEmpty || name.contains('/') then Abort.panic(InvalidActorName(name))
        else if props.mailboxCapacity < 1 then
            Abort.panic(InvalidBehavior(s"mailbox capacity must be positive, was ${props.mailboxCapacity}"))
        else
            parent.system.isAlive.flatMap { isAlive =>
                if !isAlive then Abort.fail(ActorSystemTerminated(parent.system.name))
                else reserveAndStart(parent, name, behavior, props)
            }
    end spawnOn

    private def reserveAndStart[N](
        parent: Cell[?],
        name: String,
        behavior: Behavior[N],
        props: Props
    )(using Frame): ActorRef[N] < (Async & Scope & Abort[SpawnFailure]) =
        parent.reserve(name).flatMap { ok =>
            if !ok then Abort.fail(ActorNameExists(name, parent.path))
            else
                val started =
                    for
                        child <- Cell.open[N](parent.system, parent.path / name, Present(parent))
                        // Props supervision is the outermost strategy. Inner `supervise` wrappers run first.
                        // https://pekko.apache.org/docs/pekko/current/typed/fault-tolerance.html
                        wrapped = Behavior.supervise(behavior).onFailure(props.supervision)
                        ref <- start(child, wrapped, props.mailboxCapacity)
                        _   <- parent.publish(name, ref)
                    yield ref
                Abort.recover[SpawnFailure](
                    error => parent.unreserve(name).andThen(Abort.fail(error)),
                    ex => parent.unreserve(name).andThen(Abort.panic(ex))
                )(started)
        }
    end reserveAndStart

    private def start[M](cell: Cell[M], behavior: Behavior[M], capacity: Int)(using Frame): ActorRef[M] < (Async & Scope) =
        for
            refPromise   <- Promise.init[ActorRef[M], Abort[Nothing]]
            actorPromise <- Promise.init[Running, Abort[Nothing]]
            actor        <- Actor.run(capacity)(drive(cell, refPromise, actorPromise, behavior))
            _            <- actorPromise.completeDiscard(Result.succeed(actor.asInstanceOf[Running]))
            ref          <- awaitPromise(refPromise)
        yield ref
    end start

    private def awaitPromise[A](promise: Promise[A, Abort[Nothing]])(using Frame): A < Async =
        promise.getResult.flatMap {
            case Result.Success(value) => value
            case Result.Panic(error)   => Abort.panic(error)
            case Result.Failure(_)     => Abort.panic(InvalidBehavior("promise failed unexpectedly"))
        }
    end awaitPromise

    private def drive[M](
        cell: Cell[M],
        refPromise: Promise[ActorRef[M], Abort[Nothing]],
        actorPromise: Promise[Running, Abort[Nothing]],
        behavior: Behavior[M]
    )(using Frame): Unit < Actor.Context[Mail] =
        awaitPromise(actorPromise).flatMap { actor =>
            cell.bind(actor).andThen {
                Actor.self[Mail].flatMap { subject =>
                    val ref     = LiveRef[M](cell, subject)
                    val context = ContextImpl[M](cell, ref)
                    cell.attach(subject, context).andThen {
                        refPromise.completeDiscard(Result.succeed(ref)).andThen {
                            // Final cleanup runs when this behavior returns or the fiber is interrupted.
                            // https://getkyo.io/latest/kyo-actor/
                            Scope.ensure(cell.cleanup).andThen {
                                Abort.recover[Any](
                                    error => cell.fail(failureCause(error)),
                                    ex =>
                                        ex match
                                            case _: Interrupted => ()
                                            case other          => cell.fail(other)
                                )(incarnations(cell, ref, context, behavior, 0))
                            }
                        }
                    }
                }
            }
        }
    end drive

    // A restart builds a fresh incarnation: children of the previous one are stopped and `setup` runs again.
    // The message that failed is not replayed.
    // https://pekko.apache.org/docs/pekko/current/typed/fault-tolerance.html
    private def incarnations[M](
        cell: Cell[M],
        ref: ActorRef[M],
        context: ContextImpl[M],
        behavior: Behavior[M],
        restarts: Int
    )(using Frame): Unit < (Actor.Context[Mail] & Async & Scope) =
        val (core, strategies) = peel(behavior)
        val propsStrategy      = strategies.last
        Scope.run(runOne(cell, ref, context, core, strategies, propsStrategy, restarts)).flatMap {
            case Outcome.Restart() =>
                // `Scope.run` already stopped children spawned inside the incarnation. This also
                // stops children linked through `spawn`.
                // https://pekko.apache.org/docs/pekko/current/typed/fault-tolerance.html
                cell.stopChildren.andThen(incarnations(cell, ref, context, behavior, restarts + 1))
            case Outcome.Halt(cause) =>
                cause match
                    case Present(error) => cell.fail(error)
                    case Absent         => ()
            case Outcome.Continue(_) => ()
        }
    end incarnations

    private def runOne[M](
        cell: Cell[M],
        ref: ActorRef[M],
        context: ContextImpl[M],
        behavior: Behavior[M],
        strategies: List[SupervisorStrategy],
        propsStrategy: SupervisorStrategy,
        restarts: Int
    )(using Frame): Outcome[M] < (Actor.Context[Mail] & Async & Scope) =
        cell.noteProps(propsStrategy).andThen {
            cell.setStrategies(strategies, restarts).andThen {
                Abort.run[Any] {
                    expand(context, behavior, Absent).flatMap { ready =>
                        cell.remember(ready).andThen {
                            arm(cell, ref).andThen {
                                Actor.receiveLoop[Mail](Outcome.Continue(ready): Outcome[M]) { (mail, state) =>
                                    state match
                                        case Outcome.Continue(current) =>
                                            onMail(cell, ref, context, current, mail).map {
                                                case next: Outcome.Continue[M] => Loop.continue(next)
                                                case done                      => Loop.done(done)
                                            }
                                        case done => Loop.done(done)
                                }
                            }
                        }
                    }
                }.flatMap { result =>
                    result match
                        case Result.Success(outcome: Outcome.Continue[M]) =>
                            // Mailbox closed: the receive loop finished without an explicit halt.
                            // https://getkyo.io/latest/kyo-actor/
                            Outcome.Halt[M](Absent)
                        case Result.Success(outcome) => outcome
                        case Result.Failure(error)   =>
                            decide(cell, applyFailure(strategies, failureCause(error), restarts), Outcome.Restart[M]())
                        case Result.Panic(_: Interrupted) => Outcome.Halt[M](Absent)
                        case Result.Panic(error)          =>
                            decide(cell, applyFailure(strategies, error, restarts), Outcome.Restart[M]())
                }
            }
        }
    end runOne

    private def onMail[M](
        cell: Cell[M],
        ref: ActorRef[M],
        context: ContextImpl[M],
        ready: Ready[M],
        mail: Mail
    )(using Frame): Outcome[M] < Behavior.Effect =
        mail match
            case Mail.Stop             => Outcome.Halt[M](Absent)
            case Mail.Escalated(cause) =>
                failureOutcome(cell, ready, cause)
            case Mail.Signal(signal) =>
                onSignal(cell, context, ready, signal)
            case Mail.User(message) =>
                val effect: Behavior[M] < Behavior.Effect =
                    ready match
                        case Ready.Receive(onMessage, _) =>
                            // `tell` is the only producer of `Mail.User`, and it enqueues this actor's `M`.
                            onMessage(context, message.asInstanceOf[M])
                        case Ready.Ignore(_) => Behavior.same[M]
                effect.flatMap { next =>
                    Abort.run[Any](settle(cell, context, ready, next)).flatMap { result =>
                        result match
                            case Result.Success(outcome) => rememberOutcome(cell, ref, outcome)
                            case Result.Failure(error)   => failureOutcome(cell, ready, failureCause(error))
                            case Result.Panic(error)     => failureOutcome(cell, ready, error)
                    }
                }
        end match
    end onMail

    private def onSignal[M](
        cell: Cell[M],
        context: ContextImpl[M],
        ready: Ready[M],
        signal: Signal
    )(using Frame): Outcome[M] < Behavior.Effect =
        ready.signals match
            case Present(handler) =>
                Abort.run[Any](handler(context, signal).flatMap(next => settle(cell, context, ready, next))).flatMap {
                    case Result.Success(outcome) => outcome
                    case Result.Failure(error)   => failureOutcome(cell, ready, failureCause(error))
                    case Result.Panic(error)     => failureOutcome(cell, ready, error)
                }
            case Absent =>
                signal match
                    // Watching without a handler fails the watcher. The supervisor then applies.
                    // https://pekko.apache.org/docs/pekko/current/typed/fault-tolerance.html
                    case Signal.Terminated(watched, _) =>
                        failureOutcome(cell, ready, DeathPact(context.path, watched.path))
                    case Signal.PostStop => Outcome.Continue(ready)
        end match
    end onSignal

    private def failureOutcome[M](cell: Cell[M], ready: Ready[M], cause: Throwable)(using Frame): Outcome[M] < Async =
        cell.strategies.get.flatMap { strategies =>
            cell.restarts.get.flatMap { restarts =>
                // Resume: the failed message is dropped and the current behavior stays.
                // https://pekko.apache.org/docs/pekko/current/typed/fault-tolerance.html
                val resume = cell.remember(ready).andThen(Outcome.Continue(ready))
                decide(cell, applyFailure(strategies, cause, restarts), resume)
            }
        }
    end failureOutcome

    private def decide[M](cell: Cell[M], decision: Decision, onResume: Outcome[M] < Async)(using Frame): Outcome[M] < Async =
        decision match
            case Decision.Resume      => onResume
            case Decision.Restart     => Outcome.Restart[M]()
            case Decision.Stop(cause) => Outcome.Halt[M](cause)
            // No strategy accepted the failure, so the parent actor sees it.
            // https://pekko.apache.org/docs/pekko/current/typed/fault-tolerance.html
            case Decision.Escalate(cause) => escalate(cell, cause).andThen(Outcome.Halt[M](Present(cause)))
    end decide

    private def rememberOutcome[M](cell: Cell[M], ref: ActorRef[M], outcome: Outcome[M])(using Frame): Outcome[M] < Async =
        outcome match
            case continue: Outcome.Continue[M] =>
                cell.remember(continue.ready).andThen(arm(cell, ref)).andThen(continue)
            case other => other
    end rememberOutcome

    private def applyFailure(strategies: List[SupervisorStrategy], cause: Throwable, restarts: Int): Decision =
        @tailrec def loop(rest: List[SupervisorStrategy]): Decision =
            rest match
                case Nil              => Decision.Escalate(cause)
                case strategy :: tail =>
                    strategy.decide(cause) match
                        case Directive.Resume  => Decision.Resume
                        case Directive.Restart =>
                            strategy.maxRestarts match
                                case Present(max) if restarts >= max => Decision.Stop(Present(cause))
                                case _                               => Decision.Restart
                        case Directive.Stop     => Decision.Stop(Present(cause))
                        case Directive.Escalate => loop(tail)
        loop(strategies)
    end applyFailure

    private def settle[M](
        cell: Cell[M],
        context: ContextImpl[M],
        current: Ready[M],
        next: Behavior[M]
    )(using Frame): Outcome[M] < Behavior.Effect =
        next match
            case _: Behavior.Inst.Same[?]    => Outcome.Continue(current)
            case _: Behavior.Inst.Stopped[?] => Outcome.Halt[M](Absent)
            case other                       =>
                val (core, userStrategies) = peel(other)
                cell.propsStrategy.flatMap { props =>
                    expand(context, core, Absent).flatMap { ready =>
                        cell.setStrategyList(userStrategies :+ props).andThen(Outcome.Continue(ready))
                    }
                }
    end settle

    private def expand[M](
        context: ContextImpl[M],
        behavior: Behavior[M],
        signals: Maybe[Behavior.SignalHandle[M]]
    )(using Frame): Ready[M] < Behavior.Effect =
        behavior match
            case Behavior.Inst.WithSignal(inner, handler) =>
                val chosen = signals match
                    case Present(_) => signals
                    case Absent     => Present(handler)
                expand(context, inner, chosen)
            case Behavior.Inst.Supervised(inner, _) => expand(context, inner, signals)
            case Behavior.Inst.Setup(init)          => init(context).flatMap(next => expand(context, next, signals))
            case Behavior.Inst.Receive(onMessage)   => Ready.Receive(onMessage, signals)
            case _: Behavior.Inst.Ignore[?]         => Ready.Ignore(signals)
            case _: Behavior.Inst.Stopped[?]        => Abort.panic(InvalidBehavior("Behavior.stopped does not accept messages"))
            case _: Behavior.Inst.Same[?]           =>
                Abort.panic(InvalidBehavior("Behavior.same is only valid as the result of handling a message"))
    end expand

    // Innermost strategy is the head of the list. Props wraps the behavior, so it is last.
    // https://pekko.apache.org/docs/pekko/current/typed/fault-tolerance.html
    private def peel[M](behavior: Behavior[M]): (Behavior[M], List[SupervisorStrategy]) =
        behavior match
            case Behavior.Inst.Supervised(inner, strategy) =>
                val (core, innerStrategies) = peel(inner)
                (core, innerStrategies :+ strategy)
            case Behavior.Inst.WithSignal(inner, handler) =>
                val (core, strategies) = peel(inner)
                (Behavior.Inst.WithSignal(core, handler), strategies)
            case other => (other, Nil)
    end peel

    // The timer is reset after every user message, including the timeout message.
    // https://pekko.apache.org/docs/pekko/current/typed/interaction-patterns.html
    private def arm[M](cell: Cell[M], ref: ActorRef[M])(using Frame): Unit < Async =
        cell.timeoutGeneration.incrementAndGet.flatMap { generation =>
            cell.timeout.get.flatMap {
                case Absent                    => ()
                case Present((after, message)) =>
                    // Unscoped: the generation check makes a stale sleeper a no-op, including after stop.
                    Fiber.initUnscoped {
                        Async.sleep(after).andThen {
                            cell.timeoutGeneration.get.flatMap { current =>
                                cell.stopping.get.flatMap { isStopping =>
                                    if current == generation && !isStopping then
                                        ref.tell(message).handle(Abort.recover[TellFailure](_ => (), ex => Abort.panic(ex)))
                                    else ()
                                }
                            }
                        }
                    }.unit
            }
        }
    end arm

    private def failureCause(error: Any)(using Frame): Throwable =
        error match
            case throwable: Throwable => throwable
            case other                => ActorAborted(String.valueOf(other))

    final private[actors] class SystemShell[M](
        val name: String,
        alive: AtomicBoolean
    ) extends ActorSystem[M]:
        private var guardianCell: Cell[M]    = scala.compiletime.uninitialized
        private var guardianRef: ActorRef[M] = scala.compiletime.uninitialized

        private[Dispatch] def attach(cell: Cell[M], ref: ActorRef[M]): Unit =
            guardianCell = cell
            guardianRef = ref

        def guardian: ActorRef[M] = guardianRef

        def spawn[N](behavior: Behavior[N], child: String, props: Props)(using
            Frame
        ): ActorRef[N] < (Async & Scope & Abort[SpawnFailure]) =
            spawnOn(guardianCell, child, behavior, props)

        def terminate(using Frame): Unit < Async =
            alive.compareAndSet(true, false).flatMap { _ =>
                guardianRef.stop.andThen(guardianRef.awaitTerminated)
            }

        def whenTerminated(using Frame): Unit < Async = guardianRef.awaitTerminated

        private[Dispatch] def isAlive(using Frame): Boolean < Sync = alive.get

        private[Dispatch] def markDead(using Frame): Unit < Sync = alive.set(false)
    end SystemShell

    final private class Cell[M](
        val system: SystemShell[?],
        val path: ActorPath,
        val parent: Maybe[Cell[?]],
        val stopping: AtomicBoolean,
        val cleaned: AtomicBoolean,
        val children: AtomicRef[Map[String, Slot]],
        val watchers: AtomicRef[List[Registration]],
        val cause: AtomicRef[Maybe[Throwable]],
        val actor: AtomicRef[Maybe[Running]],
        var subject: Maybe[Actor.Subject[Mail]],
        var context: Maybe[ContextImpl[M]],
        val signals: AtomicRef[Maybe[Behavior.SignalHandle[M]]],
        val timeout: AtomicRef[Maybe[(Duration, M)]],
        val timeoutGeneration: AtomicInt,
        val terminated: Promise[Unit, Abort[Nothing]],
        val strategies: AtomicRef[List[SupervisorStrategy]],
        val restarts: AtomicInt,
        val props: AtomicRef[SupervisorStrategy]
    ):
        val id: AnyRef = this

        def bind(handle: Running)(using Frame): Unit < Sync = actor.set(Present(handle))

        def attach(mailbox: Actor.Subject[Mail], ctx: ContextImpl[M])(using Frame): Unit < Sync =
            Sync.defer {
                subject = Present(mailbox)
                context = Present(ctx)
            }

        def remember(ready: Ready[M])(using Frame): Unit < Sync = signals.set(ready.signals)

        def noteProps(strategy: SupervisorStrategy)(using Frame): Unit < Sync = props.set(strategy)

        def propsStrategy(using Frame): SupervisorStrategy < Sync = props.get

        def setStrategies(next: List[SupervisorStrategy], restartCount: Int)(using Frame): Unit < Sync =
            strategies.set(next).andThen(restarts.set(restartCount))

        def setStrategyList(next: List[SupervisorStrategy])(using Frame): Unit < Sync = strategies.set(next)

        def fail(error: Throwable)(using Frame): Unit < Sync =
            cause.get.flatMap {
                case Present(_) => ()
                case Absent     => cause.set(Present(error))
            }

        def reserve(name: String)(using Frame): Boolean < Sync =
            Loop.foreach {
                children.get.flatMap { map =>
                    if map.contains(name) then Loop.done(false)
                    else
                        children.compareAndSet(map, map.updated(name, Slot.Pending)).map { won =>
                            if won then Loop.done(true) else Loop.continue
                        }
                }
            }

        def publish(name: String, ref: ActorRef[?])(using Frame): Unit < Sync =
            Loop.foreach {
                children.get.flatMap { map =>
                    children.compareAndSet(map, map.updated(name, Slot.Ready(ref))).map { won =>
                        if won then Loop.done[Unit, Unit](()) else Loop.continue
                    }
                }
            }

        def unreserve(name: String)(using Frame): Unit < Sync =
            Loop.foreach {
                children.get.flatMap { map =>
                    map.get(name) match
                        case Some(Slot.Pending) =>
                            children.compareAndSet(map, map - name).map { won =>
                                if won then Loop.done[Unit, Unit](()) else Loop.continue
                            }
                        case _ => Loop.done[Unit, Unit](())
                }
            }

        def unlink(name: String, ref: ActorRef[?])(using Frame): Unit < Sync =
            Loop.foreach {
                children.get.flatMap { map =>
                    map.get(name) match
                        case Some(Slot.Ready(current)) if current eq ref =>
                            children.compareAndSet(map, map - name).map { won =>
                                if won then Loop.done[Unit, Unit](()) else Loop.continue
                            }
                        case _ => Loop.done[Unit, Unit](())
                }
            }

        def isChild(child: ActorRef[?])(using Frame): Boolean < Sync =
            children.get.map { map =>
                map.get(child.name) match
                    case Some(Slot.Ready(current)) => current eq child
                    case _                         => false
            }

        def stopChildren(using Frame): Unit < Async =
            children.getAndSet(Map.empty).flatMap { map =>
                Kyo.foreachDiscard(map.values.toList) {
                    case Slot.Ready(ref) => ref.stop.andThen(ref.awaitTerminated)
                    case Slot.Pending    => ()
                }
            }

        def requestStop(using Frame): Unit < Async =
            stopping.set(true).andThen {
                val queued: Unit < Async =
                    subject match
                        case Present(mailbox) =>
                            mailbox.send(Mail.Stop).handle(Abort.recover[Closed](_ => (), ex => Abort.panic(ex)))
                        case Absent => ()
                queued.andThen {
                    actor.get.flatMap {
                        case Present(handle) => handle.closeDiscard
                        case Absent          => ()
                    }
                }
            }

        def watch(key: AnyRef, callback: Maybe[Throwable] => Unit < Async)(using Frame): Unit < Async =
            for
                cancelled <- AtomicBoolean.init(false)
                gate      <- AtomicBoolean.init(false)
                registration = Registration(key, cancelled, gate, callback)
                already <- cleaned.get
                _       <-
                    if already then cause.get.flatMap(reason => registration.fire(reason))
                    else
                        watchers.updateAndGet(list => registration :: list.filterNot(_.key eq key)).andThen {
                            cleaned.get.flatMap { now =>
                                if now then cause.get.flatMap(reason => registration.fire(reason))
                                else ()
                            }
                        }
            yield ()

        def unwatch(key: AnyRef)(using Frame): Unit < Sync =
            watchers.get.flatMap { list =>
                Kyo.foreachDiscard(list) { registration =>
                    if registration.key eq key then registration.cancel else ()
                }
            }

        def cleanup(using Frame): Unit < Async =
            cleaned.compareAndSet(false, true).flatMap { won =>
                if !won then ()
                else
                    stopping.set(true).andThen {
                        val marked: Unit < Sync =
                            parent match
                                case Absent => system.markDead
                                case _      => ()
                        marked.andThen {
                            stopChildren.andThen {
                                postStop.andThen {
                                    notifyWatchers.andThen {
                                        unlinkFromParent.andThen {
                                            terminated.completeDiscard(Result.succeed(())).unit
                                        }
                                    }
                                }
                            }
                        }
                    }
            }

        private def postStop(using Frame): Unit < Async =
            signals.get.flatMap {
                case Absent           => ()
                case Present(handler) =>
                    context match
                        case Absent       => ()
                        case Present(ctx) =>
                            Scope.run {
                                handler(ctx, Signal.PostStop).handle(
                                    Abort.recover[Any](_ => Behavior.stopped[M], _ => Behavior.stopped[M])
                                )
                            }.unit
            }

        private def notifyWatchers(using Frame): Unit < Async =
            cause.get.flatMap { reason =>
                watchers.getAndSet(Nil).flatMap { list =>
                    Kyo.foreachDiscard(list)(_.fire(reason))
                }
            }

        private def unlinkFromParent(using Frame): Unit < Sync =
            parent match
                case Absent         => ()
                case Present(owner) =>
                    context match
                        case Present(ctx) => owner.unlink(path.name, ctx.self)
                        case Absent       => ()

        def awaitStop(using Frame): Unit < Async =
            awaitPromise(terminated)
    end Cell

    private object Cell:
        def open[M](system: SystemShell[?], path: ActorPath, parent: Maybe[Cell[?]])(using Frame): Cell[M] < Sync =
            for
                stopping          <- AtomicBoolean.init(false)
                cleaned           <- AtomicBoolean.init(false)
                children          <- AtomicRef.init(Map.empty[String, Slot])
                watchers          <- AtomicRef.init(List.empty[Registration])
                cause             <- AtomicRef.init(Absent: Maybe[Throwable])
                actor             <- AtomicRef.init(Absent: Maybe[Running])
                signals           <- AtomicRef.init(Absent: Maybe[Behavior.SignalHandle[M]])
                timeout           <- AtomicRef.init(Absent: Maybe[(Duration, M)])
                timeoutGeneration <- AtomicInt.init(0)
                terminated        <- Promise.init[Unit, Abort[Nothing]]
                strategies        <- AtomicRef.init(List.empty[SupervisorStrategy])
                restarts          <- AtomicInt.init(0)
                props             <- AtomicRef.init(SupervisorStrategy.stop)
            yield Cell[M](
                system,
                path,
                parent,
                stopping,
                cleaned,
                children,
                watchers,
                cause,
                actor,
                Absent,
                Absent,
                signals,
                timeout,
                timeoutGeneration,
                terminated,
                strategies,
                restarts,
                props
            )
    end Cell

    final private class LiveRef[M](private[Dispatch] val cell: Cell[M], subject: Actor.Subject[Mail]) extends ActorRef[M]:
        private[actors] def id: AnyRef = cell.id
        def path: ActorPath            = cell.path

        def tell(message: M)(using Frame): Unit < (Async & Abort[TellFailure]) =
            ensureAccepting.andThen(deliver(Mail.User(message)))

        def ask[R](timeout: Duration)(build: ActorRef[R] => M)(using Frame): R < (Async & Abort[AskFailure]) =
            ensureAccepting.andThen {
                cell.actor.get.flatMap {
                    case Absent         => Abort.fail(ActorClosed(path))
                    case Present(actor) =>
                        val call =
                            Async.timeout(timeout) {
                                actor.ask[R] { replyTo =>
                                    Mail.User(build(ReplyRef(replyTo)))
                                }
                            }
                        call.handle(Abort.recover[Timeout | Closed](
                            {
                                case _: Timeout => Abort.fail(AskTimeout(path, timeout))
                                case _          => Abort.fail(ActorClosed(path))
                            },
                            ex => Abort.panic(ex)
                        ))
                }
            }

        def stop(using Frame): Unit < Async = cell.requestStop

        def awaitTerminated(using Frame): Unit < Async = cell.awaitStop

        private def ensureAccepting(using Frame): Unit < (Async & Abort[ActorSystemTerminated | ActorClosed]) =
            cell.system.isAlive.flatMap { isAlive =>
                if !isAlive then Abort.fail(ActorSystemTerminated(cell.system.name))
                else
                    cell.stopping.get.flatMap { isStopping =>
                        if isStopping then Abort.fail(ActorClosed(path)) else ()
                    }
            }

        private def deliver(mail: Mail)(using Frame): Unit < (Async & Abort[TellFailure]) =
            // `tell` suspends on a full mailbox because Kyo `Actor.send` waits for capacity.
            // https://getkyo.io/latest/kyo-actor/
            subject.send(mail).handle(Abort.recover[Closed](_ => Abort.fail(ActorClosed(path)), ex => Abort.panic(ex)))
    end LiveRef

    final private class ReplyRef[A](subject: Actor.Subject[A]) extends ActorRef[A]:
        private[actors] val token      = new AnyRef
        private[actors] def id: AnyRef = token
        def path: ActorPath            = ActorPath.root / "temp"

        def tell(message: A)(using Frame): Unit < (Async & Abort[TellFailure]) =
            subject.send(message).handle(Abort.recover[Closed](_ => Abort.fail(ActorClosed(path)), ex => Abort.panic(ex)))

        def ask[R](timeout: Duration)(build: ActorRef[R] => A)(using Frame): R < (Async & Abort[AskFailure]) =
            Abort.panic(InvalidBehavior("ask is not supported on an ask reply ref"))

        def stop(using Frame): Unit < Async = ()

        def awaitTerminated(using Frame): Unit < Async = ()
    end ReplyRef

    final private class ContextImpl[M](cell: Cell[M], val self: ActorRef[M]) extends ActorContext[M]:
        def system: ActorSystem[?] = cell.system

        def spawn[N](behavior: Behavior[N], name: String, props: Props)(using
            Frame
        ): ActorRef[N] < (Async & Scope & Abort[SpawnFailure]) =
            spawnOn(cell, name, behavior, props)

        def stop[N](child: ActorRef[N])(using Frame): Unit < Async =
            cell.isChild(child).flatMap { isChild =>
                if isChild then child.stop
                else Abort.panic(InvalidBehavior(s"${child.path.render} is not a child of ${cell.path.render}"))
            }

        def watch[N](actor: ActorRef[N])(using Frame): Unit < Async =
            watchCell(actor) { cause =>
                deliver(Mail.Signal(Signal.Terminated(actor, cause)))
            }

        def watchWith[N](actor: ActorRef[N], message: M)(using Frame): Unit < Async =
            watchCell(actor) { _ =>
                self.tell(message).handle(Abort.recover[TellFailure](_ => (), ex => Abort.panic(ex)))
            }

        def unwatch[N](actor: ActorRef[N])(using Frame): Unit < Sync =
            actor match
                case live: LiveRef[?] => live.cell.unwatch(cell.id)
                case _                => ()

        def setReceiveTimeout(after: Duration, message: M)(using Frame): Unit < Async =
            if after.toNanos <= 0 then cancelReceiveTimeout
            else cell.timeout.set(Present((after, message))).andThen(arm(cell, self))

        def cancelReceiveTimeout(using Frame): Unit < Async =
            cell.timeout.set(Absent).andThen(cell.timeoutGeneration.incrementAndGet).unit

        private def watchCell[N](actor: ActorRef[N])(callback: Maybe[Throwable] => Unit < Async)(using Frame): Unit < Async =
            actor match
                case live: LiveRef[?] => live.cell.watch(cell.id, callback)
                // Reply refs never terminate, matching temporary ask refs.
                // https://pekko.apache.org/docs/pekko/current/typed/interaction-patterns.html
                case _ => ()

        private def deliver(mail: Mail)(using Frame): Unit < Async =
            cell.subject match
                case Present(mailbox) =>
                    mailbox.send(mail).handle(Abort.recover[Closed](_ => (), ex => Abort.panic(ex)))
                case Absent => ()
    end ContextImpl

    private def escalate[M](cell: Cell[M], cause: Throwable)(using Frame): Unit < Async =
        cell.parent match
            case Absent          => ()
            case Present(parent) =>
                parent.subject match
                    case Absent           => ()
                    case Present(mailbox) =>
                        mailbox.send(Mail.Escalated(cause)).handle(Abort.recover[Closed](_ => (), ex => Abort.panic(ex)))
    end escalate
end Dispatch
