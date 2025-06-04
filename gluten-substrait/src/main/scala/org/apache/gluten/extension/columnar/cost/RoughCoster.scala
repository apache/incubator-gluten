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
package org.apache.gluten.extension.columnar.cost

import org.apache.gluten.execution.{ColumnarToCarrierRowExecBase, RowToColumnarExecBase}
import org.apache.gluten.extension.columnar.transition.{ColumnarToColumnarLike, ColumnarToRowLike, RowToColumnarLike}
import org.apache.gluten.utils.PlanUtil

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, NamedExpression}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}

object RoughCoster extends LongCoster {
  override def kind(): LongCostModel.Kind = LongCostModel.Rough

  override def selfCostOf(node: SparkPlan): Option[Long] = {
    Some(selfCostOf0(node))
  }

  private def selfCostOf0(node: SparkPlan): Long = {
    node match {
      case ColumnarWriteFilesExec.OnNoopLeafPath(_) => 0
      case ProjectExec(projectList, _) if projectList.forall(isCheapExpression) =>
        // Make trivial ProjectExec has the same cost as ProjectExecTransform to reduce unnecessary
        // c2r and r2c.
        10L
      case r2c: RowToColumnarExecBase if hasComplexTypes(r2c.schema) =>
        // Avoid moving computation back to native when transition has complex types in schema.
        // Such transitions are observed to be extremely expensive as of now.
        Long.MaxValue
      case _: ColumnarToCarrierRowExecBase => 0L
      case ColumnarToRowLike(_) => 10L
      case RowToColumnarLike(_) => 10L
      case ColumnarToColumnarLike(_) => 5L
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

  private def hasComplexTypes(schema: StructType): Boolean = {
    schema.exists(_.dataType match {
      case _: StructType => true
      case _: ArrayType => true
      case _: MapType => true
      case _ => false
    })
  }
}
