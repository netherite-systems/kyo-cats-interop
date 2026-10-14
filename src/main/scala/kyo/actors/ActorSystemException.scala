package kyo.actors

import kyo.*

/** Failure raised by the actor system.
  *
  * Leaves are grouped by the operation that produces them so a caller can handle one row
  * (`Abort[SpawnFailure]`, `Abort[TellFailure]`, `Abort[AskFailure]`) and leave the others in place.
  *
  * @param message
  *   text built from the leaf's typed fields
  * @param cause
  *   optional underlying explanation
  */
sealed abstract class ActorSystemException(message: String, cause: String | Throwable = "")(using Frame)
    extends KyoException(message, cause)

/** `spawn` rejected the actor. */
sealed trait SpawnFailure

/** `tell` could not enqueue the message. */
sealed trait TellFailure

/** `ask` did not produce a reply. */
sealed trait AskFailure

/** An actor with this name is already registered under `parent`.
  *
  * @param name
  *   the requested child name
  * @param parent
  *   the parent path that already contains `name`
  */
final case class ActorNameExists(name: String, parent: ActorPath)(using Frame)
    extends ActorSystemException(s"actor '$name' already exists under ${parent.render}")
    with SpawnFailure

/** The system has been terminated, so it no longer accepts work.
  *
  * @param system
  *   the actor system name
  */
final case class ActorSystemTerminated(system: String)(using Frame)
    extends ActorSystemException(s"actor system '$system' is terminated")
    with SpawnFailure
    with TellFailure
    with AskFailure

/** The target actor has stopped and its mailbox no longer accepts messages.
  *
  * @param path
  *   the stopped actor
  */
final case class ActorClosed(path: ActorPath)(using Frame)
    extends ActorSystemException(s"actor ${path.render} is stopped")
    with TellFailure
    with AskFailure

/** A request/reply `ask` exceeded its deadline.
  *
  * @param path
  *   the actor that was asked
  * @param timeout
  *   the deadline that elapsed
  */
final case class AskTimeout(path: ActorPath, timeout: Duration)(using Frame)
    extends ActorSystemException(s"ask to ${path.render} timed out after $timeout")
    with AskFailure

/** A behavior aborted with a value that is not itself a `Throwable`.
  *
  * Supervisors match on `Throwable`. A typed `Abort` value is wrapped so the same decider can see it.
  *
  * @param error
  *   `String.valueOf` of the aborted value
  */
final case class ActorAborted(error: String)(using Frame)
    extends ActorSystemException(s"actor aborted with '$error'")

/** `Behavior.same` was used where a runnable behavior is required.
  *
  * @param detail
  *   where the misuse was detected
  */
final case class InvalidBehavior(detail: String)(using Frame) extends ActorSystemException(detail)

/** A programmer-supplied actor name is empty or contains `/`.
  *
  * @param name
  *   the rejected name
  */
final case class InvalidActorName(name: String)(using Frame)
    extends ActorSystemException(s"invalid actor name '$name'")

/** The watcher was not prepared for the watched actor to stop.
  *
  * @param watcher
  *   the actor that called `watch` and did not handle `Terminated`
  * @param watched
  *   the actor that stopped
  */
final case class DeathPact(watcher: ActorPath, watched: ActorPath)(using Frame)
    extends ActorSystemException(s"${watcher.render} watched ${watched.render}, which terminated")
