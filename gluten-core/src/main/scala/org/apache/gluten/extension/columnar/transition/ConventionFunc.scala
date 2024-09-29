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

import org.apache.gluten.backend.Backend
import org.apache.gluten.extension.columnar.transition.ConventionReq.KnownChildrenConventions
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.execution.{ColumnarToRowExec, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

/** ConventionFunc is a utility to derive [[Convention]] or [[ConventionReq]] from a query plan. */
sealed trait ConventionFunc {
  def conventionOf(plan: SparkPlan): Convention
  def conventionReqOf(plan: SparkPlan): ConventionReq
}

object ConventionFunc {
  trait Override {
    def rowTypeOf: PartialFunction[SparkPlan, Convention.RowType] = PartialFunction.empty
    def batchTypeOf: PartialFunction[SparkPlan, Convention.BatchType] = PartialFunction.empty
    def conventionReqOf: PartialFunction[SparkPlan, ConventionReq] = PartialFunction.empty
  }

  object Override {
    object Empty extends Override
  }

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
    val batchOverride = newOverride()
    new BuiltinFunc(batchOverride)
  }

  private def newOverride(): Override = {
    synchronized {
      if (ignoreBackend) {
        // For testing
        return Override.Empty
      }
    }
    Backend.get().convFuncOverride()
  }

  private class BuiltinFunc(o: Override) extends ConventionFunc {
    import BuiltinFunc._
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
          Backend.get().defaultBatchType
        } else {
          Convention.BatchType.None
        }
        val conv = Convention.of(rowType, batchType)
        conv
      case other =>
        val conv = Convention.of(rowTypeOf(other), batchTypeOf(other))
        conv
    }

    private def rowTypeOf(plan: SparkPlan): Convention.RowType = {
      val out = o.rowTypeOf.applyOrElse(plan, rowTypeOf0)
      out
    }

    private def rowTypeOf0(plan: SparkPlan): Convention.RowType = {
      val out = plan match {
        case k: Convention.KnownRowType =>
          k.rowType()
        case _ if SparkShimLoader.getSparkShims.supportsRowBased(plan) =>
          Convention.RowType.VanillaRow
        case _ =>
          Convention.RowType.None
      }
      assert(
        out == Convention.RowType.None || plan.isInstanceOf[Convention.KnownRowType] ||
          SparkShimLoader.getSparkShims.supportsRowBased(plan))
      out
    }

    private def batchTypeOf(plan: SparkPlan): Convention.BatchType = {
      val out = o.batchTypeOf.applyOrElse(plan, batchTypeOf0)
      out
    }

    private def batchTypeOf0(plan: SparkPlan): Convention.BatchType = {
      val out = plan match {
        case k: Convention.KnownBatchType =>
          k.batchType()
        case _ if plan.supportsColumnar =>
          Convention.BatchType.VanillaBatch
        case _ =>
          Convention.BatchType.None
      }
      assert(out == Convention.BatchType.None || plan.supportsColumnar)
      out
    }

    override def conventionReqOf(plan: SparkPlan): ConventionReq = {
      val req = o.conventionReqOf.applyOrElse(plan, conventionReqOf0)
      req
    }

    private def conventionReqOf0(plan: SparkPlan): ConventionReq = plan match {
      case k: KnownChildrenConventions =>
        val reqs = k.requiredChildrenConventions().distinct
        // This can be a temporary restriction.
        assert(
          reqs.size == 1,
          "KnownChildrenConventions#requiredChildrenConventions should output the same element" +
            " for all children")
        reqs.head
      case RowToColumnarLike(_) =>
        ConventionReq.of(
          ConventionReq.RowType.Is(Convention.RowType.VanillaRow),
          ConventionReq.BatchType.Any)
      case ColumnarToRowExec(_) =>
        ConventionReq.of(
          ConventionReq.RowType.Any,
          ConventionReq.BatchType.Is(Convention.BatchType.VanillaBatch))
      case write: DataWritingCommandExec if SparkShimLoader.getSparkShims.isPlannedV1Write(write) =>
        // To align with ApplyColumnarRulesAndInsertTransitions#insertTransitions
        ConventionReq.any
      case u: UnionExec =>
        // We force vanilla union to output row data to get best compatibility with vanilla Spark.
        // As a result it's a common practice to rewrite it with GlutenPlan for offloading.
        ConventionReq.of(
          ConventionReq.RowType.Is(Convention.RowType.VanillaRow),
          ConventionReq.BatchType.Any)
      case other =>
        // In the normal case, children's convention should follow parent node's convention.
        // Note, we don't have consider C2R / R2C here since they are already removed by
        // RemoveTransitions.
        val thisConv = conventionOf0(other)
        thisConv.asReq()
    }
  }

  private object BuiltinFunc {
    implicit private class ConventionOps(conv: Convention) {
      def asReq(): ConventionReq = {
        val rowTypeReq = conv.rowType match {
          case Convention.RowType.None => ConventionReq.RowType.Any
          case r => ConventionReq.RowType.Is(r)
        }

        val batchTypeReq = conv.batchType match {
          case Convention.BatchType.None => ConventionReq.BatchType.Any
          case b => ConventionReq.BatchType.Is(b)
        }
        ConventionReq.of(rowTypeReq, batchTypeReq)
      }
    }
  }
}
