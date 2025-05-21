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
import org.apache.gluten.extension.columnar.transition.{Convention, ConventionReq}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.ColumnarToRowTransition
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}

/** The operator that converts columnar batches to [[BatchCarrierRow]]s. */
abstract class ColumnarToCarrierRowExecBase extends ColumnarToRowTransition with GlutenPlan {
  import ColumnarToCarrierRowExecBase._

  override def batchType(): Convention.BatchType = Convention.BatchType.None

  override def requiredChildConvention(): Seq[ConventionReq] = {
    List(ConventionReq.ofBatch(ConventionReq.BatchType.Is(fromBatchType())))
  }

  protected def fromBatchType(): Convention.BatchType

  override lazy val metrics: Map[String, SQLMetric] =
    Map(
      "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "number of input batches"),
      "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")
    )

  override protected def doExecute(): RDD[InternalRow] = {
    val numInputBatches = longMetric("numInputBatches")
    val numOutputRows = longMetric("numOutputRows")

    child.executeColumnar().mapPartitions {
      itr =>
        itr.flatMap {
          b: ColumnarBatch =>
            numInputBatches += 1
            val numRows = b.numRows()
            if (numRows == 0) {
              Nil
            } else {
              val carrierRows = new Array[BatchCarrierRow](numRows)
              for (i <- 0 until numRows - 1) {
                carrierRows(i) = new PlaceholderRow()
              }
              carrierRows(numRows - 1) = new SerializableTerminalRow(b)
              numOutputRows += carrierRows.length
              carrierRows
            }
        }
    }
  }

  override def output: Seq[Attribute] = child.output
}

object ColumnarToCarrierRowExecBase {
  private class SerializableTerminalRow(batch: ColumnarBatch) extends TerminalRow {
    @transient
    private var _batch = batch

    override def batch(): ColumnarBatch = _batch

    override def withNewBatch(batch: ColumnarBatch): TerminalRow = new SerializableTerminalRow(
      batch)

    override def copy(): InternalRow = {
      val copied = BackendsApiManager.getSparkPlanExecApiInstance.copyColumnarBatch(_batch)
      new SerializableTerminalRow(copied)
    }

    @throws(classOf[IOException])
    private def writeObject(output: ObjectOutputStream): Unit = {
      BackendsApiManager.getSparkPlanExecApiInstance.serializeColumnarBatch(output, _batch)
    }

    @throws(classOf[IOException])
    private def readObject(input: ObjectInputStream): Unit = {
      _batch = BackendsApiManager.getSparkPlanExecApiInstance.deserializeColumnarBatch(input)
    }
  }
}
