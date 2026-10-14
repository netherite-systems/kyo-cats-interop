package kyo.actors

import kyo.*

/** Operations an actor may perform while it is handling a message or starting up.
  *
  * The context is only valid inside that actor's behavior. `spawn` creates a child of this actor.
  * `stop` stops a direct child. `watch` / `watchWith` observe another actor's permanent stop, not
  * a supervisor restart.
  * See https://pekko.apache.org/docs/pekko/current/typed/actor-lifecycle.html
  *
  * @tparam M
  *   this actor's message type
  */
abstract class ActorContext[M]:

    /** This actor's ref. */
    def self: ActorRef[M]

    /** System that owns this actor. */
    def system: ActorSystem[?]

    /** [[self]]'s path. */
    def path: ActorPath = self.path

    /** Spawn a child named `name` under this actor.
      *
      * Fails with [[ActorNameExists]] when `name` is already taken. An empty name, or a name
      * containing `/`, panics: that is a programming error.
      */
    def spawn[N](behavior: Behavior[N], name: String, props: Props = Props.default)(using
        Frame
    ): ActorRef[N] < (Async & Scope & Abort[SpawnFailure])

    /** Stop a direct child. Panics when `child` was not spawned by this actor. */
    def stop[N](child: ActorRef[N])(using Frame): Unit < Async

    /** Receive [[Signal.Terminated]] when `actor` stops.
      *
      * With no [[Behavior.onSignal]] handler, that signal fails this actor with [[DeathPact]] and
      * the failure goes through this actor's supervisor.
      * See https://pekko.apache.org/docs/pekko/current/typed/fault-tolerance.html
      */
    def watch[N](actor: ActorRef[N])(using Frame): Unit < Async

    /** `tell` `message` to this actor when `actor` stops permanently. */
    def watchWith[N](actor: ActorRef[N], message: M)(using Frame): Unit < Async

    /** Stop delivering termination notices for `actor` to this actor. */
    def unwatch[N](actor: ActorRef[N])(using Frame): Unit < Sync

    /** Deliver `message` as a user message when no user message arrives for `after`.
      *
      * The timer is reset after every user message, including the timeout message itself, until
      * [[cancelReceiveTimeout]]. `Duration.Zero` cancels the timer.
      * See https://pekko.apache.org/docs/pekko/current/typed/interaction-patterns.html
      */
    def setReceiveTimeout(after: Duration, message: M)(using Frame): Unit < Async

    /** Stop delivering receive-timeout messages. */
    def cancelReceiveTimeout(using Frame): Unit < Async
end ActorContext
