package org.apache.gluten.extension.columnar.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

import scala.collection.mutable.ListBuffer

sealed trait AdaptiveContext {
  def enableAdaptiveContext(): Unit
  def isAdaptiveContext(): Boolean
  def setAdaptiveContext(): Unit
  def resetAdaptiveContext(): Unit
  def setOriginalPlan(plan: SparkPlan): Unit
  def originalPlan(): SparkPlan
  def resetOriginalPlan(): Unit
}

object AdaptiveContext {
  def apply(session: SparkSession): AdaptiveContext = new AdaptiveContextImpl(session)

  private val GLUTEN_IS_ADAPTIVE_CONTEXT = "gluten.isAdaptiveContext"

  // This is an empirical value, may need to be changed for supporting other versions of spark.
  private val aqeStackTraceIndex = 18

  private class AdaptiveContextImpl(session: SparkSession) extends AdaptiveContext {
    // Holds the original plan for possible entire fallback.
    private val localOriginalPlans: ThreadLocal[ListBuffer[SparkPlan]] =
      ThreadLocal.withInitial(() => ListBuffer.empty[SparkPlan])
    private val localIsAdaptiveContextFlags: ThreadLocal[ListBuffer[Boolean]] =
      ThreadLocal.withInitial(() => ListBuffer.empty[Boolean])

    // Just for test use.
    override def enableAdaptiveContext(): Unit = {
      session.sparkContext.setLocalProperty(GLUTEN_IS_ADAPTIVE_CONTEXT, "true")
    }

    override def isAdaptiveContext(): Boolean =
      Option(session.sparkContext.getLocalProperty(GLUTEN_IS_ADAPTIVE_CONTEXT))
        .getOrElse("false")
        .toBoolean ||
        localIsAdaptiveContextFlags.get().head

    override def setAdaptiveContext(): Unit = {
      val traceElements = Thread.currentThread.getStackTrace
      assert(
        traceElements.length > aqeStackTraceIndex,
        s"The number of stack trace elements is expected to be more than $aqeStackTraceIndex")
      // ApplyColumnarRulesAndInsertTransitions is called by either QueryExecution or
      // AdaptiveSparkPlanExec. So by checking the stack trace, we can know whether
      // columnar rule will be applied in adaptive execution context. This part of code
      // needs to be carefully checked when supporting higher versions of spark to make
      // sure the calling stack has not been changed.
      localIsAdaptiveContextFlags
        .get()
        .prepend(
          traceElements(aqeStackTraceIndex).getClassName
            .equals(AdaptiveSparkPlanExec.getClass.getName))
    }

    override def resetAdaptiveContext(): Unit =
      localIsAdaptiveContextFlags.get().remove(0)

    override def setOriginalPlan(plan: SparkPlan): Unit = {
      localOriginalPlans.get().prepend(plan)
    }

    override def originalPlan(): SparkPlan = {
      val plan = localOriginalPlans.get().head
      assert(plan != null)
      plan
    }

    override def resetOriginalPlan(): Unit = localOriginalPlans.get().remove(0)
  }
}
