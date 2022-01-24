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

import com.intel.oap.GazelleJniConfig
import com.intel.oap.expression.{ConverterUtils, ExpressionConverter, ExpressionTransformer}
import com.intel.oap.substrait.rel.RelBuilder
import com.intel.oap.substrait.SubstraitContext

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch

class BatchScanExecTransformer(output: Seq[AttributeReference], @transient scan: Scan)
    extends BatchScanExec(output, scan) with TransformSupport {

  val filterExprs: Seq[Expression] = if (scan.isInstanceOf[FileScan]) {
    scan.asInstanceOf[FileScan].dataFilters
  } else {
    throw new UnsupportedOperationException(s"${scan.getClass.toString} is not supported")
  }

  override def supportsColumnar(): Boolean =
    super.supportsColumnar && GazelleJniConfig.getConf.enableColumnarIterator

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numInputBatches" -> SQLMetrics.createMetric(sparkContext, "input_batches"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "output_batches"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_batchscan"),
    "inputSize" -> SQLMetrics.createSizeMetric(sparkContext, "input size in bytes"))

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  override def canEqual(other: Any): Boolean = other.isInstanceOf[BatchScanExecTransformer]

  override def equals(other: Any): Boolean = other match {
    case that: BatchScanExecTransformer =>
      (that canEqual this) && super.equals(that)
    case _ => false
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    null
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {
    Seq((this, null))
  }

  override def getStreamedLeafPlan: SparkPlan = {
    this
  }

  override def getChild: SparkPlan = {
    null
  }

  override def doValidate(): Boolean = false

  override def doTransform(context: SubstraitContext): TransformContext = {
    val typeNodes = ConverterUtils.getTypeNodeFromAttributes(output)
    val nameList = new java.util.ArrayList[String]()
    for (attr <- output) {
      nameList.add(attr.name)
    }
    // Will put all filter expressions into an AND expression
    // val functionId = context.registerFunction("AND")
    val transformer = filterExprs.reduceLeftOption(And).map(
      ExpressionConverter.replaceWithExpressionTransformer(_, output)
    )
    val filterNodes = transformer.map(
      _.asInstanceOf[ExpressionTransformer].doTransform(context.registeredFunction))

    // val partNode = LocalFilesBuilder.makeLocalFiles(index, paths, starts, lengths)
    val relNode = RelBuilder.makeReadRel(typeNodes, nameList, filterNodes.getOrElse(null), context)
    TransformContext(output, output, relNode)
  }
}
