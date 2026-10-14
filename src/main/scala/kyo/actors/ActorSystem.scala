package kyo.actors

import kyo.{internal as _, *}

/** Root of an actor hierarchy.
  *
  * `init` registers the system with the enclosing [[kyo.Scope]]. Leaving that scope, or calling
  * [[ActorSystem.terminate]], stops the guardian and every actor spawned from it. The guardian
  * itself is the `/user` actor.
  * See https://getkyo.io/latest/kyo-actor/ and https://github.com/cloudmark/cats-actors/blob/master/README.md
  *
  * Persistence, clustering, and remoting are intentionally absent.
  *
  * @tparam M
  *   the guardian's message type
  */
abstract class ActorSystem[M]:

    /** Name passed to [[ActorSystem.init]]. It is not part of actor paths. */
    def name: String

    /** The `/user` actor. */
    def guardian: ActorRef[M]

    /** Spawn a top-level actor under `/user`.
      *
      * This is the scoped equivalent of cats-actors `system.actorOf`.
      * See https://github.com/cloudmark/cats-actors/blob/master/README.md
      */
    def spawn[N](behavior: Behavior[N], name: String, props: Props = Props.default)(using
        Frame
    ): ActorRef[N] < (Async & Scope & Abort[SpawnFailure])

    /** Stop the guardian and its children, then wait until they have stopped. */
    def terminate(using Frame): Unit < Async

    /** Wait until the guardian has stopped. */
    def whenTerminated(using Frame): Unit < Async
end ActorSystem

object ActorSystem:

    /** Start a system named `name` whose `/user` guardian runs `guardian`.
      *
      * The system is stopped when the enclosing scope closes.
      *
      * @param name
      *   diagnostic name of the system
      * @param guardian
      *   behavior of the `/user` actor
      */
    def init[M](name: String)(guardian: Behavior[M])(using Frame): ActorSystem[M] < (Async & Scope) =
        kyo.actors.internal.Dispatch.system(name)(guardian)

    /** Start a system named `"default"`. */
    def init[M](guardian: Behavior[M])(using Frame): ActorSystem[M] < (Async & Scope) =
        init("default")(guardian)
end ActorSystem
