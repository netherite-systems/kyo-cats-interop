# kyo-actors

A small typed actor system on [Kyo](https://getkyo.io) `1.0.0-RC7` (the current release on Maven Central). Kyo's [`kyo-actor`](https://getkyo.io/latest/kyo-actor/) module is the mailbox. This library adds the system around it: named actors, behaviors, ask, and supervision.

The shape follows [Pekko Typed](https://pekko.apache.org/docs/pekko/current/typed/actor-lifecycle.html) behaviors and [cats-actors](https://github.com/cloudmark/cats-actors/blob/master/README.md): a system is a scoped resource, `spawn` is `actorOf`, and an unmatched failure escalates.

Requires JDK 25, because Kyo 1.0.0-RC7 ships Java 25 bytecode.

```scala
import kyo.*
import kyo.actors.*

enum Counter:
    case Inc
    case Get(replyTo: ActorRef[Int])

def counter(n: Int): Behavior[Counter] =
    Behavior.receiveMessage {
        case Counter.Inc           => counter(n + 1)
        case Counter.Get(replyTo)  => replyTo.tell(n).andThen(Behavior.same)
    }

val program: Int < (Async & Scope & Abort[Any]) =
    ActorSystem.init(Behavior.ignore[Unit]).flatMap { system =>
        for
            ref <- system.spawn(
                Behavior.supervise(counter(0)).onFailure(Directive.Restart),
                "counter"
            )
            _     <- ref.tell(Counter.Inc)
            value <- ref.ask(1.second)(Counter.Get(_))
            _     <- system.terminate
        yield value
    }
```

`ActorSystem.init` ties the system to the enclosing `Scope`. `terminate`, or leaving that scope, stops the `/user` guardian and its children.

## What is included

- `ActorSystem.init` / `terminate` / `whenTerminated`
- `spawn` with a name, a path (`/user/worker`), and `Props` (mailbox capacity, outermost supervisor)
- `Behavior.receive`, `receiveMessage`, `setup`, `same`, `stopped`, `ignore`, `supervise`, `onSignal`
- `ActorRef.tell` and `ask` (request/reply with a deadline)
- `Directive.Resume`, `Restart`, `Stop`, `Escalate`, including `withLimit`
- `watch`, `watchWith`, `unwatch`, and a receive timeout
- child stop when a parent stops or restarts

## Limits

Persistence, clustering, and remoting are out of scope.

`tell` waits when the mailbox is full (`Actor.send`), rather than returning immediately. Restart stops every linked child, then runs the original behavior from `setup` again; the failed message is not replayed. `withLimit` counts restarts for the life of the actor and is not a time window. A receive timeout is a sleeper that becomes a no-op when a newer message arrives; it is not cancelled early. Reply refs used by `ask` are not actors and cannot be watched. `PostStop` runs only when the actor stops for good, not on restart. There is no AllForOne strategy and no dead-letter channel.
