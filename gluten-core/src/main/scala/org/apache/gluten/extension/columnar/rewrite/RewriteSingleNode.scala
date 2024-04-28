package org.apache.gluten.extension.columnar.rewrite

import org.apache.gluten.extension.RewriteIn
import org.apache.spark.sql.execution.SparkPlan

/**
 * Rewrites a plan node from vanilla Spark into its alternative representation.
 *
 * Gluten's planner will pick one that is considered the best executable plan between input plan
 * and the output plan.
 *
 * Note: Only the current plan node is supposed to be open to modification. Do not access or modify
 * the children node. Tree-walking is done by caller of this trait.
 *
 * TODO: Ideally for such API we'd better to allow multiple alternative outputs.
 */
trait RewriteSingleNode {
  def rewrite(plan: SparkPlan): SparkPlan
}

object RewriteSingleNode {
  def allRules(): Seq[RewriteSingleNode] = {
    Seq(
      RewriteIn,
      RewriteMultiChildrenCount,
      RewriteCollect,
      RewriteTypedImperativeAggregate,
      PullOutPreProject,
      PullOutPostProject)
  }
}
