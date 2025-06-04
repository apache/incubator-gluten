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

import org.apache.gluten.execution.ColumnarToCarrierRowExecBase
import org.apache.gluten.extension.columnar.transition.{ColumnarToColumnarLike, ColumnarToRowLike, RowToColumnarLike}
import org.apache.gluten.utils.PlanUtil

import org.apache.spark.sql.execution.{ColumnarWriteFilesExec, ProjectExec, SparkPlan}

object LegacyCoster extends LongCoster {
  override def kind(): LongCostModel.Kind = LongCostModel.Legacy

  override def selfCostOf(node: SparkPlan): Option[Long] = {
    Some(selfCostOf0(node))
  }

  // A very rough estimation as of now. The cost model basically considers any
  // fallen back ops as having extreme high cost so offloads computations as
  // much as possible.
  private def selfCostOf0(node: SparkPlan): Long = {
    node match {
      case ColumnarWriteFilesExec.OnNoopLeafPath(_) => 0L
      case _: ColumnarToCarrierRowExecBase => 0L
      case ColumnarToRowLike(_) => 10L
      case RowToColumnarLike(_) => 10L
      case ColumnarToColumnarLike(_) => 5L
      case p if PlanUtil.isGlutenColumnarOp(p) => 10L
      // 1. 100L << 1000L, to keep the pulled out non-offload-able projects if the main op
      // turns into offload-able after pulling.
      // 2. 100L >> 10L, to offload project op itself eagerly.
      case ProjectExec(_, _) => 100L
      case p if PlanUtil.isVanillaColumnarOp(p) => 1000L
      // Other row ops. Usually a vanilla row op.
      case _ => 1000L
    }
  }
}
