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
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{SparkPlan, UnionExec}
import org.apache.spark.sql.execution.command.DataWritingCommandExec

import scala.annotation.tailrec

case class InsertTransitions(outputsColumnar: Boolean) extends Rule[SparkPlan] {
  import InsertTransitions._
  private val convFunc = ConventionFunc.create()

  override def apply(plan: SparkPlan): SparkPlan = {
    // Remove all transitions at first.
    val removed = RemoveTransitions.apply(plan)
    val filled = fillWithTransitions(removed)
    if (!outputsColumnar) {
      return Transitions.toRowPlan(filled)
    }
    Transitions.toBackendBatchPlan(filled)
  }

  private def fillWithTransitions(plan: SparkPlan): SparkPlan = plan.transformUp {
    case p => applyForNode(p)
  }

  private def applyForNode(node: SparkPlan): SparkPlan = {
    if (node.children.isEmpty) {
      return node
    }
    val convReq = childrenConvReqOf(node)
    val newChildren = node.children.map {
      child =>
        val from = convFunc.conventionOf(child)
        if (from.isNone) {
          // For example, a union op with row child and columnar child at the same time,
          // The plan is actually not executable and we cannot tell about its convention.
          child
        } else {
          val transition =
            Transition.factory.findTransition(from, convReq, Transition.notFound(node))
          val newChild = transition.apply(child)
          newChild
        }
    }
    node.withNewChildren(newChildren)
  }

  private def childrenConvReqOf(node: SparkPlan): ConventionReq = node match {
    // TODO: Consider C2C transitions as well when we have some.
    case ColumnarToRowLike(_) | RowToColumnarLike(_) =>
      // C2R / R2C here since they are already removed by
      // RemoveTransitions.
      // It's current rule's mission to add C2Rs / R2Cs on demand.
      throw new IllegalStateException("Unreachable code")
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
      val thisConv = convFunc.conventionOf(other)
      thisConv.asReq()
  }
}

object InsertTransitions {
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

object RemoveTransitions extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown { case p => removeForNode(p) }

  @tailrec
  private[transition] def removeForNode(plan: SparkPlan): SparkPlan = plan match {
    // TODO: Consider C2C transitions as well when we have some.
    case ColumnarToRowLike(child) => removeForNode(child)
    case RowToColumnarLike(child) => removeForNode(child)
    case other => other
  }
}

object Transitions {
  def insertTransitions(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan = {
    val out = InsertTransitions(outputsColumnar).apply(plan)
    out
  }

  def toRowPlan(plan: SparkPlan): SparkPlan = {
    val convFunc = ConventionFunc.create()
    val req = ConventionReq.of(
      ConventionReq.RowType.Is(Convention.RowType.VanillaRow),
      ConventionReq.BatchType.Any)
    val removed = RemoveTransitions.removeForNode(plan)
    val transition = Transition.factory.findTransition(
      convFunc.conventionOf(removed),
      req,
      Transition.notFound(removed, req))
    val out = transition.apply(removed)
    out
  }

  def toBackendBatchPlan(plan: SparkPlan): SparkPlan = {
    val backendBatchType = BackendsApiManager.getSparkPlanExecApiInstance.batchType
    val out = toBatchPlan(plan, backendBatchType)
    out
  }

  def toVanillaBatchPlan(plan: SparkPlan): SparkPlan = {
    val out = toBatchPlan(plan, Convention.BatchType.VanillaBatch)
    out
  }

  private def toBatchPlan(plan: SparkPlan, toBatchType: Convention.BatchType): SparkPlan = {
    val convFunc = ConventionFunc.create()
    val req = ConventionReq.of(ConventionReq.RowType.Any, ConventionReq.BatchType.Is(toBatchType))
    val removed = RemoveTransitions.removeForNode(plan)
    val transition = Transition.factory.findTransition(
      convFunc.conventionOf(removed),
      req,
      Transition.notFound(removed, req))
    val out = transition.apply(removed)
    out
  }
}
