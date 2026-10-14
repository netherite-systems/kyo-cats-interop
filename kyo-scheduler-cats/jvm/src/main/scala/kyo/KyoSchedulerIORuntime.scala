package kyo

import cats.effect.unsafe.IORuntime
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration

object KyoSchedulerIORuntime {

    /** `IORuntime` whose compute and blocking executors are `kyo.scheduler.Scheduler`.
      *
      * Timers stay on a two-thread `ScheduledExecutorService`. `IO.sleep` uses that scheduler, not the Kyo compute pool. The shutdown hook
      * is a no-op, so `IORuntime#shutdown` does not stop the Kyo scheduler.
      *
      * See https://typelevel.org/cats-effect/api/3.x/cats/effect/unsafe/IORuntime.html
      */
    implicit lazy val global: IORuntime = {
        // `sleep` is the Cats Effect timer hook used by `IO.sleep`.
        // See https://typelevel.org/cats-effect/api/3.x/cats/effect/unsafe/Scheduler.html
        val scheduler = new cats.effect.unsafe.Scheduler {
            val scheduledExecutor                                      = Executors.newScheduledThreadPool(2)
            def sleep(delay: FiniteDuration, task: Runnable): Runnable = {
                val t = scheduledExecutor.schedule(task, delay.toNanos, TimeUnit.NANOSECONDS)
                () => {
                    t.cancel(false)
                    ()
                }
            }
            def monotonicNanos(): Long = System.nanoTime()
            def nowMillis(): Long      = System.currentTimeMillis()
        }
        // Both runtime pools share one Kyo scheduler.
        // See https://getkyo.io/latest/kyo-scheduler/
        val kyoExecutor = kyo.scheduler.Scheduler.get.asExecutionContext
        IORuntime(kyoExecutor, kyoExecutor, scheduler, () => (), IORuntime.global.config)
    }
}
