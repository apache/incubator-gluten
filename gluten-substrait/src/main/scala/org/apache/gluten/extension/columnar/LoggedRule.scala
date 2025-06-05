package org.apache.gluten.extension.columnar

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.logging.LogLevelUtil
import org.apache.gluten.metrics.GlutenTimeMetric
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.execution.SparkPlan

/**
 * Since https://github.com/apache/incubator-gluten/pull/7606.
 */
class LoggedRule(delegate: Rule[SparkPlan])
  extends Rule[SparkPlan]
    with Logging
    with LogLevelUtil {

  override val ruleName: String = delegate.ruleName

  private def message(oldPlan: SparkPlan, newPlan: SparkPlan, millisTime: Long): String =
    if (!oldPlan.fastEquals(newPlan)) {
      s"""
         |=== Applying Rule $ruleName took $millisTime ms ===
         |${sideBySide(oldPlan.treeString, newPlan.treeString).mkString("\n")}
           """.stripMargin
    } else {
      s"Rule $ruleName has no effect, took $millisTime ms."
    }

  override def apply(plan: SparkPlan): SparkPlan = {
    val (out, millisTime) = GlutenTimeMetric.recordMillisTime(delegate.apply(plan))
    logOnLevel(GlutenConfig.get.transformPlanLogLevel, message(plan, out, millisTime))
    out
  }
}
