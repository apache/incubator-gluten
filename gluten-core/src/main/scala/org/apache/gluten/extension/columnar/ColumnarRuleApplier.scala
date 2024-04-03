package org.apache.gluten.extension.columnar

import org.apache.spark.sql.execution.SparkPlan

trait ColumnarRuleApplier {
  def apply(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan
}
