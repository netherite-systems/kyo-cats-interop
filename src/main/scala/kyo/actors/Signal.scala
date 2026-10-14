package kyo.actors

import kyo.{Signal as _, *}

/** Lifecycle notification delivered to a behavior's signal handler.
  *
  * Signals are not user messages. `receive` does not see them; install a handler with
  * [[Behavior.onSignal]] (or use [[ActorContext.watchWith]], which adapts termination into an
  * ordinary message).
  *
  * Permanent stop is what a watcher observes. A supervisor restart does not emit `Terminated`.
  * See https://pekko.apache.org/docs/pekko/current/typed/actor-lifecycle.html
  */
enum Signal derives CanEqual:

    /** The actor is stopping for good and will not process further user messages. */
    case PostStop

    /** `ref` has stopped. `cause` is present when the stop was due to a failure. */
    case Terminated(ref: ActorRef[?], cause: Maybe[Throwable])
end Signal
