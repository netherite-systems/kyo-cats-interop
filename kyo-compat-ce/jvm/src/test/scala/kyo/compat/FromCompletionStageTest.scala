package kyo.compat

import java.util.concurrent.CompletableFuture
import kyo.compat.*
import scala.util.Failure
import scala.util.Success

/** JVM-only scenarios for `CIO.fromCompletionStage`.
  *
  * `java.util.concurrent.CompletionStage` is not available on Scala.js or Scala Native, so this suite lives in the JVM source set.
  *
  * See https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/CompletionStage.html
  */
class FromCompletionStageTest extends CompatTest:

    "fcs_completed_returns_value" in run {
        val cs = CompletableFuture.completedFuture(42)
        val c  = CIO.fromCompletionStage(cs)
        c.liftToTry.map {
            case Success(42) => succeed
            case other       => fail(s"expected Success(42), got: $other")
        }
    }

    "fcs_failure_propagates" in run {
        val cs = new CompletableFuture[Int]
        cs.completeExceptionally(new RuntimeException("cs-boom"))
        val c = CIO.fromCompletionStage(cs)
        c.liftToTry.map {
            case Failure(t: Throwable) =>
                // The CompletionStage may wrap the exception in a CompletionException;
                // unwrap one level if needed.
                // See https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/CompletionException.html
                val msg = if t.getMessage == "cs-boom" then t.getMessage
                else if t.getCause != null && t.getCause.getMessage == "cs-boom" then t.getCause.getMessage
                else t.getMessage
                assert(msg == "cs-boom", s"expected cs-boom (or wrapped), got: $t")
            case other => fail(s"expected Failure(Throwable), got: $other")
        }
    }

end FromCompletionStageTest
