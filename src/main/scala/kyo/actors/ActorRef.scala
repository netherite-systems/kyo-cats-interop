package kyo.actors

import kyo.*

/** Typed handle of a living or stopped actor.
  *
  * `tell` is fire-and-forget. It suspends when the bounded mailbox is full, rather than dropping
  * the message. `ask` is request/reply with a deadline: the function receives a one-shot reply
  * ref and must `tell` exactly one response.
  * See https://getkyo.io/latest/kyo-actor/ and https://pekko.apache.org/docs/pekko/current/typed/interaction-patterns.html
  *
  * Reply refs created for `ask` are not hierarchy actors. `tell` completes the reply; `ask` on a
  * reply ref is a programming error.
  *
  * @tparam M
  *   message type
  */
abstract class ActorRef[M]:

    private[actors] def id: AnyRef

    /** Path of this actor in its system. */
    def path: ActorPath

    /** Last segment of [[path]]. */
    def name: String = path.name

    /** Enqueue `message`. Suspends while the mailbox is full.
      *
      * Fails with [[ActorClosed]] when the actor has stopped, and with [[ActorSystemTerminated]]
      * when the system has been shut down.
      */
    def tell(message: M)(using Frame): Unit < (Async & Abort[TellFailure])

    /** Send `build(replyTo)` and wait up to `timeout` for one reply.
      *
      * The reply ref accepts a single `tell`. A stopped target fails with [[ActorClosed]]. A
      * deadline fails with [[AskTimeout]]. A terminated system fails with [[ActorSystemTerminated]].
      *
      * @param timeout
      *   how long to wait for the reply
      * @param build
      *   constructs the request from the one-shot reply ref
      */
    def ask[R](timeout: Duration)(build: ActorRef[R] => M)(using Frame): R < (Async & Abort[AskFailure])

    /** Stop this actor and its children. Further `tell`s fail with [[ActorClosed]]. */
    def stop(using Frame): Unit < Async

    /** Wait until this actor has stopped. */
    def awaitTerminated(using Frame): Unit < Async

    final override def equals(other: Any): Boolean =
        other match
            case ref: ActorRef[?] => ref.id eq id
            case _                => false

    final override def hashCode: Int = id.hashCode
end ActorRef

object ActorRef:
    given [A, B]: CanEqual[ActorRef[A], ActorRef[B]] = CanEqual.derived
end ActorRef
