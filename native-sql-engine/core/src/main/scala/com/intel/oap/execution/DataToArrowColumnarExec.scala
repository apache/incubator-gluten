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

package com.intel.oap.execution

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit._

import com.intel.oap.vectorized._
import com.intel.oap.GazellePluginConfig

import org.apache.spark.{broadcast, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{Utils, UserAddedJarUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  Expression,
  SortOrder,
  UnsafeProjection
}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import scala.collection.JavaConverters._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import scala.collection.mutable.ListBuffer
import org.apache.arrow.vector.ipc.message.ArrowFieldNode
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.arrow.vector.types.pojo.Field
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.gandiva.expression._
import org.apache.arrow.gandiva.evaluator._

import org.apache.arrow.memory.ArrowBuf
import io.netty.buffer.ByteBuf
import com.google.common.collect.Lists;

import com.intel.oap.expression._
import com.intel.oap.vectorized.ExpressionEvaluator
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
case class DataToArrowColumnarExec(child: SparkPlan, numPartitions: Int) extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = UnknownPartitioning(numPartitions)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val inputRdd = BroadcastColumnarRDD(
      sparkContext,
      metrics,
      numPartitions,
      child.executeBroadcast[ColumnarHashedRelation]())
    inputRdd.mapPartitions { batches =>
      val toUnsafe = UnsafeProjection.create(output, output)
      batches.flatMap { batch =>
        numOutputBatches += 1
        numOutputRows += batch.numRows()
        batch.rowIterator().asScala.map(toUnsafe)
      }
    }
  }

  override def doExecuteBroadcast[T](): broadcast.Broadcast[T] = {
    child.executeBroadcast()
  }

  override def supportsColumnar: Boolean = true

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_datatoarrowcolumnar"))

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("numOutputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    BroadcastColumnarRDD(
      sparkContext,
      metrics,
      numPartitions,
      child.executeBroadcast[ColumnarHashedRelation]())
  }

}
