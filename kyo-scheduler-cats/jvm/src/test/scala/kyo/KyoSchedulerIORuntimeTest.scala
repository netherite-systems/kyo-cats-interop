package kyo

import cats.effect.*
import org.scalatest.freespec.AnyFreeSpec

class KyoSchedulerIORuntimeTest extends AnyFreeSpec {

    import KyoSchedulerIORuntime.global

    "compute" in {
        val thread = IO.cede.map(_ => Thread.currentThread().getName()).unsafeRunSync()
        assert(thread.contains("kyo"))
    }

    "blocking" in {
        val thread = IO.cede.flatMap(_ => IO.blocking(Thread.currentThread().getName())).unsafeRunSync()
        assert(thread.contains("kyo"))
    }

    "IOApp" in {
        val code = KyoSchedulerIOAppTest.run(Nil).unsafeRunSync()
        assert(code == ExitCode.Success)
    }
}
