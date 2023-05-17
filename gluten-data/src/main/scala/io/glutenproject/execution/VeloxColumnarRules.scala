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

package io.glutenproject.execution

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.columnarbatch.ArrowColumnarBatches
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.vectorized.ColumnarBatch

case class LoadArrowData(child: SparkPlan) extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      "LoadArrowData does not support the execute() code path.")
  }

  override def nodeName: String = "LoadArrowData"

  override def supportsColumnar: Boolean = true

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    child.executeColumnar().mapPartitions {
      itr =>
        BackendsApiManager.getIteratorApiInstance.genCloseableColumnBatchIterator(itr.map {
          cb => ArrowColumnarBatches.ensureLoaded(ArrowBufferAllocators.contextInstance(), cb)
        })
    }
  }

  override def output: Seq[Attribute] = {
    child.output
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

object VeloxColumnarRules {
  // Load when fallback to vanilla columnar to row
  // Remove it when supports all the spark type velox columnar to row
  case class LoadBeforeColumnarToRow() extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
      case c2r @ ColumnarToRowExec(_: ColumnarShuffleExchangeExec) =>
        c2r // AdaptiveSparkPlanExec.scala:536
      case c2r @ ColumnarToRowExec(_: ColumnarBroadcastExchangeExec) =>
        c2r // AdaptiveSparkPlanExec.scala:546
      case ColumnarToRowExec(child) => ColumnarToRowExec(LoadArrowData(child))
    }
  }
}
