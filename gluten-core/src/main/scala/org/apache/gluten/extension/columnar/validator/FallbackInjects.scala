package org.apache.gluten.extension.columnar.validator

import org.apache.spark.sql.execution.SparkPlan

object FallbackInjects {
  private var fallbackCondition: Option[PartialFunction[SparkPlan, Boolean]] = None

  def fallbackOn[T](condition: PartialFunction[SparkPlan, Boolean])(func: => T): T =
    synchronized {
      assert(this.fallbackCondition.isEmpty)
      this.fallbackCondition = Some(condition)
      try {
        func
      } finally {
        this.fallbackCondition = None
      }
    }

  private[validator] def shouldFallback(node: SparkPlan): Boolean = {
    fallbackCondition.exists(_.applyOrElse(node, { _: SparkPlan => false }))
  }
}
