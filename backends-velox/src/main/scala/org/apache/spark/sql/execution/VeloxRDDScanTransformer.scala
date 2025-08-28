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
package org.apache.spark.sql.execution

import org.apache.gluten.execution.ValidationResult
import org.apache.gluten.vectorized.VeloxColumnarBatch

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class VeloxRDDScanTransformer(
    outputAttributes: Seq[Attribute],
    rdd: RDD[InternalRow],
    name: String,
    override val outputPartitioning: Partitioning = UnknownPartitioning(0),
    override val outputOrdering: Seq[SortOrder]
) extends RDDScanTransformer(outputAttributes, outputPartitioning, outputOrdering) {

  override protected def doValidateInternal(): ValidationResult = {
    ValidationResult.succeeded
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    rdd.mapPartitions { it =>
      if (it.hasNext) {
        val rows = it.toArray
        val batch = VeloxColumnarBatch.from(schema, rows)
        Iterator.single(batch)
      } else {
        Iterator.empty
      }
    }
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(outputAttributes, rdd, name, outputPartitioning, outputOrdering)
}

object VeloxRDDScanTransformer {
  def replace(rdd: RDDScanExec): RDDScanTransformer =
    VeloxRDDScanTransformer(
      rdd.output,
      rdd.inputRDD,
      rdd.nodeName,
      rdd.outputPartitioning,
      rdd.outputOrdering)
}