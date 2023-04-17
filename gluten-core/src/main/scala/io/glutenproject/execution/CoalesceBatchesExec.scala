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
import io.glutenproject.expression.ConverterUtils
import io.glutenproject.extension.GlutenPlan
import io.glutenproject.substrait.`type`.TypeNode
import io.glutenproject.substrait.rel.ReadRelNode
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.util
import java.util.ArrayList

  case class CoalesceBatchesExec(child: SparkPlan) extends UnaryExecNode with GlutenPlan {

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genCoalesceBatchesMetrics(sparkContext)

  override def output: Seq[Attribute] = child.output

  override def supportsColumnar: Boolean = true

  override def nodeName: String = "CoalesceBatches"

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val recordsPerBatch = conf.arrowMaxRecordsPerBatch
    val numOutputRows = longMetric("numOutputRows")
    val numInputBatches = longMetric("numInputBatches")
    val numOutputBatches = longMetric("numOutputBatches")
    val collectTime = longMetric("collectTime")
    val concatTime = longMetric("concatTime")
    val avgCoalescedNumRows = longMetric("avgCoalescedNumRows")

    logDebug(s"xxx child is ${child.getClass},\n$child\n child.output=${child.output}")
    /*
    logDebug(s"xxx child is ${child.getClass}, " +
      s"child's child is ${child.children(0).children(0).getClass}")
    val cchild = child.children(0).children(0)
    if (cchild.isInstanceOf[HashAggregateExecBaseTransformer]) {
      logDebug(s"xxxx is a hash agg")
    }

    */


    child.executeColumnar().mapPartitions { iter =>
      logDebug(s"xxx call + getCoalesceInputAttributes")
      val (typeList, nameList) = BackendsApiManager.getTransformerApiInstance
        .getCoalesceInputAttributes(child)
      logDebug(s"xxx typeList=$typeList; nameList:$nameList")
      val schema = ReadRelNode.buildNamedStruct(typeList, nameList, null)
      BackendsApiManager.getIteratorApiInstance
        .genCoalesceIterator(iter, recordsPerBatch, numOutputRows,
          numInputBatches, numOutputBatches, collectTime, concatTime, avgCoalescedNumRows,
          schema.toByteArray)
    }
  }

  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException

  override protected def withNewChildInternal(newChild: SparkPlan): CoalesceBatchesExec =
    copy(child = newChild)
}
