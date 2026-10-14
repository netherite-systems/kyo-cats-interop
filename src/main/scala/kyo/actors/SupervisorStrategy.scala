package kyo.actors

import kyo.*
import scala.reflect.ClassTag

/** What a supervisor does with a failed child incarnation.
  *
  * The names match the classic actor directives. `Escalate` asks the next outer strategy, and
  * ultimately the parent actor, to decide.
  * See https://pekko.apache.org/docs/pekko/current/typed/fault-tolerance.html
  */
enum Directive derives CanEqual:
    case Resume
    case Restart
    case Stop
    case Escalate
end Directive

/** Decides how an actor failure is applied to that actor.
  *
  * Strategies are attached to a behavior (`Behavior.supervise`) or to [[Props]] at spawn.
  * The innermost strategy runs first. `Escalate` defers to the next outer strategy. With no
  * strategy left, the failure stops the actor and is reported to its parent.
  * See https://pekko.apache.org/docs/pekko/current/typed/fault-tolerance.html
  */
sealed abstract class SupervisorStrategy:

    /** Directive for `cause`. */
    def decide(cause: Throwable): Directive

    /** Restart budget. `Absent` restarts without a numeric cap. */
    def maxRestarts: Maybe[Int]

    /** Returns a strategy that stops after `max` restarts instead of restarting forever.
      *
      * The count is the number of restarts already performed for this actor. It is not reset when
      * a later message succeeds, and it is not limited to a time window.
      *
      * @param max
      *   how many restarts are allowed before the failure stops the actor
      */
    def withLimit(max: Int): SupervisorStrategy
end SupervisorStrategy

object SupervisorStrategy:

    /** Stop the actor on any failure. This is the default applied by [[Props]]. */
    def stop: SupervisorStrategy = fixed(Directive.Stop)

    /** Pass every failure to the parent actor.
      *
      * The parent treats that failure as its own and applies its strategy. With the default
      * strategy, the parent stops.
      * See https://pekko.apache.org/docs/pekko/current/typed/fault-tolerance.html
      */
    def escalate: SupervisorStrategy = fixed(Directive.Escalate)

    /** Drop the failed message and keep the current behavior, including its captured state. */
    def resume: SupervisorStrategy = fixed(Directive.Resume)

    /** Drop the failed message, stop children, and run the supervised behavior from the start. */
    def restart: SupervisorStrategy = fixed(Directive.Restart)

    /** Build a total decider. Every cause is handled; nothing is escalated by default. */
    def apply(decider: Throwable => Directive): SupervisorStrategy =
        oneForOne { case cause => decider(cause) }

    /** Partial decider. An unmatched cause escalates, as in cats-actors `OneForOneStrategy`.
      *
      * See https://github.com/cloudmark/cats-actors/blob/master/README.md
      *
      * @param decider
      *   directive for the causes it matches
      */
    def oneForOne(decider: PartialFunction[Throwable, Directive]): SupervisorStrategy =
        Deciding(decider, Absent)

    /** Apply `directive` only when the failure is an `E`, and escalate every other cause. */
    def onFailure[E <: Throwable: ClassTag](directive: Directive): SupervisorStrategy =
        oneForOne { case cause if summon[ClassTag[E]].runtimeClass.isInstance(cause) => directive }

    private def fixed(directive: Directive): SupervisorStrategy = Fixed(directive, Absent)

    final private case class Fixed(directive: Directive, maxRestarts: Maybe[Int]) extends SupervisorStrategy:
        def decide(cause: Throwable): Directive     = directive
        def withLimit(max: Int): SupervisorStrategy = copy(maxRestarts = Present(max))
    end Fixed

    final private case class Deciding(decider: PartialFunction[Throwable, Directive], maxRestarts: Maybe[Int]) extends SupervisorStrategy:
        def decide(cause: Throwable): Directive     = decider.applyOrElse(cause, (_: Throwable) => Directive.Escalate)
        def withLimit(max: Int): SupervisorStrategy = copy(maxRestarts = Present(max))
    end Deciding
end SupervisorStrategy
