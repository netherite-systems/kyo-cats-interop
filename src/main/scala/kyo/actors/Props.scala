package kyo.actors

import kyo.Actor

/** Spawn-time options for an actor.
  *
  * `supervision` is the outermost strategy: a strategy wrapped around the behavior with
  * [[Behavior.supervise]] is consulted first, and escalations fall through to this one.
  * The default stops the actor. An outermost [[Directive.Escalate]] reports the failure to the parent.
  * See https://pekko.apache.org/docs/pekko/current/typed/fault-tolerance.html
  *
  * @param mailboxCapacity
  *   bound on queued messages. `tell` suspends when the mailbox is full
  * @param supervision
  *   outermost failure strategy
  */
final case class Props(mailboxCapacity: Int, supervision: SupervisorStrategy) derives CanEqual:

    /** Mailbox bound. Values below 1 are rejected when the actor is spawned. */
    def withMailbox(capacity: Int): Props = copy(mailboxCapacity = capacity)

    /** Outermost supervisor. */
    def withSupervision(strategy: SupervisorStrategy): Props = copy(supervision = strategy)
end Props

object Props:

    /** Capacity [[kyo.Actor.defaultCapacity]] and [[SupervisorStrategy.stop]]. */
    def default: Props = Props(Actor.defaultCapacity, SupervisorStrategy.stop)
end Props
