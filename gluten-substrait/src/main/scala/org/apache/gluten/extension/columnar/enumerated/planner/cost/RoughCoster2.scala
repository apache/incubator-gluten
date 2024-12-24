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
package org.apache.gluten.extension.columnar.enumerated.planner.cost

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension.columnar.transition.{ColumnarToRowLike, RowToColumnarLike}
import org.apache.gluten.utils.PlanUtil

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, NamedExpression}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExecBase

// Since https://github.com/apache/incubator-gluten/pull/7686.
object RoughCoster2 extends LongCoster {
  override def kind(): LongCostModel.Kind = LongCostModel.Rough2

  override def selfCostOf(node: SparkPlan): Option[Long] = {
    Some(selfCostOf0(node))
  }

  private def selfCostOf0(node: SparkPlan): Long = {
    val sizeFactor = getSizeFactor(node)
    val opCost = node match {
      case ProjectExec(projectList, _) if projectList.forall(isCheapExpression) =>
        // Make trivial ProjectExec has the same cost as ProjectExecTransform to reduce unnecessary
        // c2r and r2c.
        1L
      case ColumnarToRowExec(_) => 1L
      case RowToColumnarExec(_) => 1L
      case ColumnarToRowLike(_) => 1L
      case RowToColumnarLike(_) =>
        // If sizeBytes is less than the threshold, the cost of RowToColumnarLike is ignored.
        if (sizeFactor == 0) 1L else GlutenConfig.get.rasRough2R2cCost
      case p if PlanUtil.isGlutenColumnarOp(p) => 1L
      case p if PlanUtil.isVanillaColumnarOp(p) => GlutenConfig.get.rasRough2VanillaCost
      // Other row ops. Usually a vanilla row op.
      case _ => GlutenConfig.get.rasRough2VanillaCost
    }
    opCost * Math.max(1, sizeFactor)
  }

  private def getSizeFactor(plan: SparkPlan): Long = {
    // Get the bytes size that the plan needs to consume.
    val sizeBytes = plan match {
      case _: DataSourceScanExec | _: DataSourceV2ScanExecBase => getStatSizeBytes(plan)
      case _: LeafExecNode => 0L
      case p => p.children.map(getStatSizeBytes).sum
    }
    sizeBytes / GlutenConfig.get.rasRough2SizeBytesThreshold
  }

  private def getStatSizeBytes(plan: SparkPlan): Long = {
    plan match {
      case a: AdaptiveSparkPlanExec => getStatSizeBytes(a.inputPlan)
      case _ =>
        plan.logicalLink match {
          case Some(logicalPlan) => logicalPlan.stats.sizeInBytes.toLong
          case _ => plan.children.map(getStatSizeBytes).sum
        }
    }
  }

  private def isCheapExpression(ne: NamedExpression): Boolean = ne match {
    case Alias(_: Attribute, _) => true
    case _: Attribute => true
    case _ => false
  }
}
