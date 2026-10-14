package kyo

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp

/** Cats-Effect [[cats.effect.IOApp]] variant that runs on the Kyo scheduler.
  *
  * Overrides `runtime` to use [[KyoSchedulerIORuntime.global]], so every fiber spawned from the resulting program is scheduled by Kyo's
  * compute pool instead of the default Cats-Effect runtime.
  *
  * See https://typelevel.org/cats-effect/api/3.x/cats/effect/IOApp.html
  */
trait KyoSchedulerIOApp extends IOApp {

    /** Cats-Effect runtime backed by the Kyo scheduler. */
    override def runtime = KyoSchedulerIORuntime.global

    /** Application entry point. Implement this with your effect program; its [[cats.effect.ExitCode]] becomes the process exit code. */
    override def run(args: List[String]): IO[ExitCode]
}
