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

import java.util

import com.google.common.collect.Lists
import com.google.protobuf.Any
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.ConverterUtils
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.extensions.ExtensionBuilder
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.vectorized.OperatorMetrics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

trait LimitMetricsUpdater extends MetricsUpdater {
  def updateNativeMetrics(operatorMetrics: OperatorMetrics): Unit
}

case class LimitTransformer(child: SparkPlan,
                            offset: Long,
                            count: Long) extends UnaryExecNode with TransformSupport {

  override lazy val metrics = Map(
    "inputRows" -> SQLMetrics.createMetric(sparkContext, "number of input rows"),
    "inputVectors" -> SQLMetrics.createMetric(sparkContext, "number of input vectors"),
    "inputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of input bytes"),
    "rawInputRows" -> SQLMetrics.createMetric(sparkContext, "number of raw input rows"),
    "rawInputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of raw input bytes"),
    "outputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "outputVectors" -> SQLMetrics.createMetric(sparkContext, "number of output vectors"),
    "outputBytes" -> SQLMetrics.createSizeMetric(sparkContext, "number of output bytes"),
    "count" -> SQLMetrics.createMetric(sparkContext, "cpu wall time count"),
    "wallNanos" -> SQLMetrics.createNanoTimingMetric(sparkContext, "totaltime_limit"),
    "peakMemoryBytes" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory bytes"),
    "numMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of memory allocations"))

  object MetricsUpdaterImpl extends LimitMetricsUpdater {
    val inputRows: SQLMetric = longMetric("inputRows")
    val inputVectors: SQLMetric = longMetric("inputVectors")
    val inputBytes: SQLMetric = longMetric("inputBytes")
    val rawInputRows: SQLMetric = longMetric("rawInputRows")
    val rawInputBytes: SQLMetric = longMetric("rawInputBytes")
    val outputRows: SQLMetric = longMetric("outputRows")
    val outputVectors: SQLMetric = longMetric("outputVectors")
    val outputBytes: SQLMetric = longMetric("outputBytes")
    val cpuCount: SQLMetric = longMetric("count")
    val wallNanos: SQLMetric = longMetric("wallNanos")
    val peakMemoryBytes: SQLMetric = longMetric("peakMemoryBytes")
    val numMemoryAllocations: SQLMetric = longMetric("numMemoryAllocations")

    override def updateNativeMetrics(operatorMetrics: OperatorMetrics): Unit = {
      if (operatorMetrics != null) {
        inputRows += operatorMetrics.inputRows
        inputVectors += operatorMetrics.inputVectors
        inputBytes += operatorMetrics.inputBytes
        rawInputRows += operatorMetrics.rawInputRows
        rawInputBytes += operatorMetrics.rawInputBytes
        outputRows += operatorMetrics.outputRows
        outputVectors += operatorMetrics.outputVectors
        outputBytes += operatorMetrics.outputBytes
        cpuCount += operatorMetrics.count
        wallNanos += operatorMetrics.wallNanos
        peakMemoryBytes += operatorMetrics.peakMemoryBytes
        numMemoryAllocations += operatorMetrics.numMemoryAllocations
      }
    }
  }

  override def supportsColumnar: Boolean = true

  override def output: Seq[Attribute] = child.output

  override def getChild: SparkPlan = child

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = child match {
    case c: TransformSupport =>
      c.columnarInputRDDs
    case _ =>
      Seq(child.executeColumnar())
  }

  override def getStreamedLeafPlan: SparkPlan = child match {
    case c: TransformSupport =>
      c.getStreamedLeafPlan
    case _ =>
      this
  }

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = child match {
    case c: TransformSupport =>
      val childPlans = c.getBuildPlans
      childPlans :+ (this, null)
    case _ =>
      Seq((this, null))
  }

  override protected def withNewChildInternal(newChild: SparkPlan): LimitTransformer =
    copy(child = newChild)

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(s"ColumnarSortExec doesn't support doExecute")
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    throw new UnsupportedOperationException(s"This operator doesn't support doExecuteColumnar().")
  }

  override def metricsUpdater(): MetricsUpdater = MetricsUpdaterImpl

  override def doValidate(): Boolean = {
    val context = new SubstraitContext
    val operatorId = context.nextOperatorId
    val relNode = try {
      getRelNode(context, operatorId, offset, count, child.output, null, true)
    } catch {
      case e: Throwable =>
        logDebug(s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}")
        return false
    }

    if (relNode != null && GlutenConfig.getConf.enableNativeValidation) {
      val planNode = PlanBuilder.makePlan(context, Lists.newArrayList(relNode))
      BackendsApiManager.getValidatorApiInstance.doValidate(planNode)
    } else {
      true
    }
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child match {
      case c: TransformSupport => c.doTransform(context)
      case _ => null
    }

    val operatorId = context.nextOperatorId
    val relNode = if (childCtx != null) {
      getRelNode(context, operatorId, offset, count, child.output, childCtx.root, false)
    } else {
      val attrList = new util.ArrayList[Attribute]()
      for (attr <- child.output) {
        attrList.add(attr)
      }
      val readRel = RelBuilder.makeReadRel(attrList, context, operatorId)
      getRelNode(context, operatorId, offset, count, child.output, readRel, false)
    }
    TransformContext(child.output, child.output, relNode)
  }

  def getRelNode(context: SubstraitContext,
                 operatorId: Long,
                 offset: Long,
                 count: Long,
                 inputAttributes: Seq[Attribute],
                 input: RelNode,
                 validation: Boolean): RelNode = {
    if (!validation) {
      RelBuilder.makeFetchRel(input, offset, count, context, operatorId)
    } else {
      val inputTypeNodes = new util.ArrayList[TypeNode]()
      for (attr <- inputAttributes) {
        inputTypeNodes.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      }
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        Any.pack(TypeBuilder.makeStruct(false, inputTypeNodes).toProtobuf))
      RelBuilder.makeFetchRel(input, offset, count, extensionNode, context, operatorId)
    }
  }
}


