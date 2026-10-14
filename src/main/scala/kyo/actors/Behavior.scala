package kyo.actors

import kyo.{Signal as _, *}

/** How an actor handles the next message.
  *
  * A behavior is a value. Message handling returns the next behavior, so state changes by
  * constructing a new behavior (the typed form of `become`). `Behavior.same` keeps the behavior
  * that is currently running, including state captured by [[Behavior.setup]].
  * See https://pekko.apache.org/docs/pekko/current/typed/actor-lifecycle.html
  *
  * Failures raised from a behavior (`Abort` or a panic) are given to the actor's supervisor.
  * `Async`, `Scope`, and `Abort` are available directly. Other effects have to be handled inside
  * the behavior before it returns.
  *
  * @tparam M
  *   message type
  */
sealed abstract class Behavior[M]

object Behavior:

    /** Effects a behavior body may leave pending.
      *
      * `Abort[Any]` is open on purpose: a body can fail with any typed error, and the supervisor
      * observes that failure. `Var` and `Env` are not included; handle them inside the body.
      */
    type Effect = Async & Scope & Abort[Any]

    private[actors] type Handle[M]       = (ActorContext[M], M) => Behavior[M] < Effect
    private[actors] type SignalHandle[M] = (ActorContext[M], Signal) => Behavior[M] < Effect
    private[actors] type SetupHandle[M]  = ActorContext[M] => Behavior[M] < Effect

    private[actors] enum Inst[M] extends Behavior[M]:
        case Receive(onMessage: Handle[M])
        case Setup(init: SetupHandle[M])
        case Ignore[M]()  extends Inst[M]
        case Same[M]()    extends Inst[M]
        case Stopped[M]() extends Inst[M]
        case Supervised(inner: Behavior[M], strategy: SupervisorStrategy)
        case WithSignal(inner: Behavior[M], onSignal: SignalHandle[M])
    end Inst

    /** Handle each message with `handle`. The function returns the next behavior. */
    def receive[M](handle: Handle[M]): Behavior[M] = Inst.Receive(handle)

    /** [[receive]] when the handler does not need [[ActorContext]]. */
    def receiveMessage[M](handle: M => Behavior[M] < Effect): Behavior[M] =
        receive((_, message) => handle(message))

    /** Run `init` when the actor starts and again on every supervisor restart.
      *
      * Children spawned from `init` are stopped before the next restart, then `init` runs again.
      * See https://pekko.apache.org/docs/pekko/current/typed/fault-tolerance.html
      */
    def setup[M](init: SetupHandle[M]): Behavior[M] = Inst.Setup(init)

    /** Keep the behavior that just handled the message. Not a valid initial behavior. */
    def same[M]: Behavior[M] = Inst.Same[M]()

    /** Stop after the current message. Child actors are stopped too. */
    def stopped[M]: Behavior[M] = Inst.Stopped[M]()

    /** Discard user messages and keep running, so the actor can still be watched and stopped. */
    def ignore[M]: Behavior[M] = Inst.Ignore[M]()

    /** Restart, stop, resume, or escalate failures of `behavior` according to `strategy`. */
    def supervise[M](behavior: Behavior[M]): Supervise[M] = Supervise(behavior)

    /** Run `handle` for [[Signal]]s. The handler replaces any signal handler wrapped inside `behavior`. */
    extension [M](behavior: Behavior[M])
        def onSignal(handle: SignalHandle[M]): Behavior[M] = Inst.WithSignal(behavior, handle)
    end extension

    /** A behavior plus the supervisor that should wrap it. */
    final class Supervise[M](behavior: Behavior[M]):

        /** Use `strategy` for every failure of `behavior`. */
        def onFailure(strategy: SupervisorStrategy): Behavior[M] = Inst.Supervised(behavior, strategy)

        /** Apply one directive to every failure. */
        def onFailure(directive: Directive): Behavior[M] =
            directive match
                case Directive.Resume   => onFailure(SupervisorStrategy.resume)
                case Directive.Restart  => onFailure(SupervisorStrategy.restart)
                case Directive.Stop     => onFailure(SupervisorStrategy.stop)
                case Directive.Escalate => onFailure(SupervisorStrategy(_ => Directive.Escalate))

        /** Apply `directive` only when the failure is an `E`. Other failures escalate. */
        def onFailureOf[E <: Throwable: scala.reflect.ClassTag](directive: Directive): Behavior[M] =
            onFailure(SupervisorStrategy.onFailure[E](directive))
    end Supervise
end Behavior
