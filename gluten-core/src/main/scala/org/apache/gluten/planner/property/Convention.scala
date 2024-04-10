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
package org.apache.gluten.planner.property

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.columnar.ColumnarTransitions
import org.apache.gluten.planner.plan.GlutenPlanModel.GroupLeafExec
import org.apache.gluten.ras.{Property, PropertyDef}
import org.apache.gluten.ras.rule.{RasRule, Shape, Shapes}
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.PlanUtil

import org.apache.spark.sql.execution.{ColumnarToRowExec, InputAdapter, RowToColumnarExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

sealed trait Convention extends Property[SparkPlan] {
  override def definition(): PropertyDef[SparkPlan, _ <: Property[SparkPlan]] = {
    ConventionDef
  }

  override def satisfies(other: Property[SparkPlan]): Boolean = other match {
    case Conventions.ANY => true
    case c: Convention => c == this
    case _ => throw new IllegalStateException()
  }
}

object Conventions {
  // FIXME: Velox and CH should have different conventions?
  case object ROW_BASED extends Convention
  case object VANILLA_COLUMNAR extends Convention
  case object GLUTEN_COLUMNAR extends Convention
  case object ANY extends Convention
}

object ConventionDef extends PropertyDef[SparkPlan, Convention] {
  // TODO: Should the convention-transparent ops (e.g., aqe shuffle read) support
  //  convention-propagation. Probably need to refactor getChildrenPropertyRequirements.
  override def getProperty(plan: SparkPlan): Convention = plan match {
    case _: GroupLeafExec => throw new IllegalStateException()
    case ColumnarToRowExec(child) => Conventions.ROW_BASED
    case RowToColumnarExec(child) => Conventions.VANILLA_COLUMNAR
    case ColumnarTransitions.ColumnarToRowLike(child) => Conventions.ROW_BASED
    case ColumnarTransitions.RowToColumnarLike(child) => Conventions.GLUTEN_COLUMNAR
    case p if PlanUtil.outputNativeColumnarData(p) => Conventions.GLUTEN_COLUMNAR
    case p if PlanUtil.isVanillaColumnarOp(p) => Conventions.VANILLA_COLUMNAR
    case p if SparkShimLoader.getSparkShims.supportsRowBased(p) => Conventions.ROW_BASED
    case _ => throw new IllegalStateException()
  }

  override def getChildrenConstraints(
      constraint: Property[SparkPlan],
      plan: SparkPlan): Seq[Convention] = plan match {
    case ColumnarToRowExec(child) => Seq(Conventions.VANILLA_COLUMNAR)
    case ColumnarTransitions.ColumnarToRowLike(child) => Seq(Conventions.GLUTEN_COLUMNAR)
    case ColumnarTransitions.RowToColumnarLike(child) => Seq(Conventions.ROW_BASED)
    case p if canPropagateConvention(p) =>
      plan.children.map(_ => constraint.asInstanceOf[Convention])
    case _ =>
      val conv = getProperty(plan)
      plan.children.map(_ => conv)
  }

  override def any(): Convention = Conventions.ANY

  private def canPropagateConvention(plan: SparkPlan): Boolean = plan match {
    case p: AQEShuffleReadExec => true
    case p: QueryStageExec => true
    case p: ReusedExchangeExec => true
    case p: InputAdapter => true
    case p: WholeStageCodegenExec => true
    case _ => false
  }
}

case class ConventionEnforcerRule(reqConv: Convention) extends RasRule[SparkPlan] {
  override def shift(node: SparkPlan): Iterable[SparkPlan] = {
    if (node.output.isEmpty) {
      // Disable transitions for node that has output with empty schema.
      return List.empty
    }
    val conv = ConventionDef.getProperty(node)
    if (conv.satisfies(reqConv)) {
      return List.empty
    }
    (conv, reqConv) match {
      case (Conventions.VANILLA_COLUMNAR, Conventions.ROW_BASED) =>
        List(ColumnarToRowExec(node))
      case (Conventions.ROW_BASED, Conventions.VANILLA_COLUMNAR) =>
        List(RowToColumnarExec(node))
      case (Conventions.GLUTEN_COLUMNAR, Conventions.ROW_BASED) =>
        List(BackendsApiManager.getSparkPlanExecApiInstance.genColumnarToRowExec(node))
      case (Conventions.ROW_BASED, Conventions.GLUTEN_COLUMNAR) =>
        val attempt = BackendsApiManager.getSparkPlanExecApiInstance.genRowToColumnarExec(node)
        if (attempt.doValidate().isValid) {
          List(attempt)
        } else {
          List.empty
        }
      case (Conventions.VANILLA_COLUMNAR, Conventions.GLUTEN_COLUMNAR) =>
        List(
          BackendsApiManager.getSparkPlanExecApiInstance.genRowToColumnarExec(
            ColumnarToRowExec(node)))
      case (Conventions.GLUTEN_COLUMNAR, Conventions.VANILLA_COLUMNAR) =>
        List(
          RowToColumnarExec(
            BackendsApiManager.getSparkPlanExecApiInstance.genColumnarToRowExec(node)))
      case _ => List.empty
    }
  }

  override def shape(): Shape[SparkPlan] = Shapes.fixedHeight(1)
}
