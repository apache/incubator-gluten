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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.extension.columnar.transition.Convention

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{LeafExecNode, RangeExec, SparkPlan}
// scalastyle:off println
abstract class RangeExecBaseTransformer(
    start: Long,
    end: Long,
    step: Long,
    numSlices: Int,
    numElements: BigInt,
    outputAttributes: Seq[Attribute],
    child: Seq[SparkPlan])
  extends LeafExecNode
  with ValidatablePlan {

  println(s"[arnavb] RangeExecBaseTransformer invoked 123")

  override def output: Seq[Attribute] = outputAttributes

  override protected def doValidateInternal(): ValidationResult = {
    // scalastyle:off println
    println("[arnavb] doValidateInternal called.")
    ValidationResult.succeeded
  }

  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType

  override def rowType0(): Convention.RowType = Convention.RowType.None

  override protected def doExecute()
      : org.apache.spark.rdd.RDD[org.apache.spark.sql.catalyst.InternalRow] = {
    // scalastyle:off println
    println("doexecute called.")
    throw new UnsupportedOperationException(s"This operator doesn't support doExecute().")
  }
}

object RangeExecBaseTransformer {
  println(s"[arnavb] RangeExecBaseTransformer invoked")
  def from(rangeExec: RangeExec): RangeExecBaseTransformer = {
    println(s"[arnavb] RangeExecBaseTransformer 1invoked")
    BackendsApiManager.getSparkPlanExecApiInstance
      .genRangeExecTransformer(
        rangeExec.start,
        rangeExec.end,
        rangeExec.step,
        rangeExec.numSlices,
        rangeExec.numElements,
        rangeExec.output,
        rangeExec.children
      )
  }
}
