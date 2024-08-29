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
package org.apache.gluten.planner.cost

import org.apache.gluten.extension.columnar.enumerated.RemoveFilter
import org.apache.gluten.extension.columnar.transition.{ColumnarToRowLike, RowToColumnarLike}
import org.apache.gluten.utils.PlanUtil

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, NamedExpression}
import org.apache.spark.sql.execution.{ColumnarToRowExec, ProjectExec, RowToColumnarExec, SparkPlan}

class RoughCostModel extends LongCostModel {

  override def selfLongCostOf(node: SparkPlan): Long = {
    node match {
      case _: RemoveFilter.NoopFilter =>
        // To make planner choose the tree that has applied rule PushFilterToScan.
        0L
      case ProjectExec(projectList, _) if projectList.forall(isCheapExpression) =>
        // Make trivial ProjectExec has the same cost as ProjectExecTransform to reduce unnecessary
        // c2r and r2c.
        10L
      case ColumnarToRowExec(_) => 10L
      case RowToColumnarExec(_) => 10L
      case ColumnarToRowLike(_) => 10L
      case RowToColumnarLike(_) => 10L
      case p if PlanUtil.isGlutenColumnarOp(p) => 10L
      case p if PlanUtil.isVanillaColumnarOp(p) => 1000L
      // Other row ops. Usually a vanilla row op.
      case _ => 1000L
    }
  }

  private def isCheapExpression(ne: NamedExpression): Boolean = ne match {
    case Alias(_: Attribute, _) => true
    case _: Attribute => true
    case _ => false
  }
}
