/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.extension.columnar.transition

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.columnar.transition.Convention.{BatchType, RowType}
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

/** ConventionFunc is a utility to derive [[Convention]] from a query plan. */
trait ConventionFunc {
  def conventionOf(plan: SparkPlan): Convention
}

object ConventionFunc {
  type BatchOverride = PartialFunction[SparkPlan, BatchType]

  // For testing, to make things work without a backend loaded.
  private var ignoreBackend: Boolean = false

  // Visible for testing
  def ignoreBackend[T](body: => T): T = synchronized {
    assert(!ignoreBackend)
    ignoreBackend = true
    try {
      body
    } finally {
      ignoreBackend = false
    }
  }

  def create(): ConventionFunc = {
    synchronized {
      if (ignoreBackend) {
        // For testing
        return new BuiltinFunc(PartialFunction.empty)
      }
    }
    val batchOverride = BackendsApiManager.getSparkPlanExecApiInstance.batchTypeFunc()
    new BuiltinFunc(batchOverride)
  }

  private class BuiltinFunc(o: BatchOverride) extends ConventionFunc {

    override def conventionOf(plan: SparkPlan): Convention = {
      val conv = conventionOf0(plan)
      conv
    }

    private def conventionOf0(plan: SparkPlan): Convention = plan match {
      case p if canPropagateConvention(p) =>
        val childrenConventions = p.children.map(conventionOf0).distinct
        if (childrenConventions.size > 1) {
          childrenConventions.reduce(_ && _)
        } else {
          assert(childrenConventions.size == 1)
          childrenConventions.head
        }
      case q: QueryStageExec => conventionOf0(q.plan)
      case r: ReusedExchangeExec => conventionOf0(r.child)
      case a: AdaptiveSparkPlanExec =>
        val rowType = rowTypeOf(a)
        val batchType = if (a.supportsColumnar) {
          // By default, we execute columnar AQE with backend batch output.
          // See org.apache.gluten.extension.columnar.transition.InsertTransitions.apply
          BackendsApiManager.getSparkPlanExecApiInstance.batchType
        } else {
          BatchType.None
        }
        val conv = Convention.of(rowType, batchType)
        conv
      case other =>
        val conv = Convention.of(rowTypeOf(other), batchTypeOf(other))
        conv
    }

    private def rowTypeOf(plan: SparkPlan): RowType = {
      if (!SparkShimLoader.getSparkShims.supportsRowBased(plan)) {
        return RowType.None
      }
      RowType.VanillaRow
    }

    private def batchTypeOf(plan: SparkPlan): BatchType = {
      if (!plan.supportsColumnar) {
        return BatchType.None
      }
      o.applyOrElse(
        plan,
        (p: SparkPlan) =>
          p match {
            case g: Convention.KnownBatchType => g.batchType()
            case _ => BatchType.VanillaBatch
          }
      )
    }
  }
}
