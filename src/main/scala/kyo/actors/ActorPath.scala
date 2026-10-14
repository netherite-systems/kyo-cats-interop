package kyo.actors

import kyo.*

/** Hierarchical name of an actor, rooted at `/`.
  *
  * The user guardian lives at `/user`. Actors spawned from the system or from a parent are appended
  * as further segments, so a child named `worker` of `/user` is `/user/worker`.
  *
  * @param segments
  *   path segments under the root, outermost first
  */
final case class ActorPath(segments: Chunk[String]) derives CanEqual:

    /** Last segment, or `/` for the root. */
    def name: String = segments.lastMaybe.fold("/")(identity)

    /** Parent path, absent at the root. */
    def parent: Maybe[ActorPath] =
        if segments.isEmpty then Absent
        else Present(ActorPath(segments.dropRight(1)))

    /** Child path for `child`. */
    def /(child: String): ActorPath = ActorPath(segments.append(child))

    /** Akka-style rendering, for example `/user/worker`. */
    def render: String =
        if segments.isEmpty then "/"
        else segments.toList.mkString("/", "/", "")

    override def toString: String = render
end ActorPath

object ActorPath:

    /** The unnamed root, `/`. */
    val root: ActorPath = ActorPath(Chunk.empty)

    /** The user guardian, `/user`. */
    val user: ActorPath = root / "user"
end ActorPath
