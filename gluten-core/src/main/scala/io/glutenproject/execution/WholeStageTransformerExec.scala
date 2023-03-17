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

import com.google.common.collect.Lists
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression._
import io.glutenproject.metrics.{MetricsUpdater, NoopMetricsUpdater}
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.plan.{PlanBuilder, PlanNode}
import io.glutenproject.substrait.rel.RelNode
import io.glutenproject.test.TestStats
import io.glutenproject.utils.{LogLevelUtil, SubstraitPlanPrinterUtil}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._
import scala.collection.mutable

case class TransformContext(
    inputAttributes: Seq[Attribute],
    outputAttributes: Seq[Attribute],
    root: RelNode)

case class WholestageTransformContext(
    inputAttributes: Seq[Attribute],
    outputAttributes: Seq[Attribute],
    root: PlanNode,
    substraitContext: SubstraitContext = null)

trait TransformSupport extends SparkPlan with LogLevelUtil {

  lazy val validateFailureLogLevel = GlutenConfig.getConf.validateFailureLogLevel
  lazy val printStackOnValidateFailure = GlutenConfig.getConf.printStackOnValidateFailure

  /**
   * Validate whether this SparkPlan supports to be transformed into substrait node in Native Code.
   */
  def doValidate(): Boolean = false

  def logValidateFailure(msg: => String, e: Throwable): Unit = {
    if (printStackOnValidateFailure) {
      logOnLevel(validateFailureLogLevel, msg, e)
    } else {
      logOnLevel(validateFailureLogLevel, msg)
    }

  }

  /**
   * Returns all the RDDs of ColumnarBatch which generates the input rows.
   *
   * @note
   *   Right now we support up to two RDDs
   */
  def columnarInputRDDs: Seq[RDD[ColumnarBatch]]

  def getBuildPlans: Seq[(SparkPlan, SparkPlan)]

  def getStreamedLeafPlan: SparkPlan

  def doTransform(context: SubstraitContext): TransformContext = {
    throw new UnsupportedOperationException(
      s"This operator doesn't support doTransform with SubstraitContext.")
  }

  def metricsUpdater(): MetricsUpdater =

  def getColumnarInputRDDs(plan: SparkPlan): Seq[RDD[ColumnarBatch]] = {
    plan match {
      case c: TransformSupport =>
        c.columnarInputRDDs
      case _ =>
        Seq(plan.executeColumnar())
    }
  }
}

case class WholeStageTransformerExec(child: SparkPlan)(val transformStageId: Int)
  extends UnaryExecNode with TransformSupport with LogLevelUtil {

  // For WholeStageCodegen-like operator, only pipeline time will be handled in graph plotting.
  // See SparkPlanGraph.scala:205 for reference.
  override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genWholeStageTransformerMetrics(sparkContext)

  val sparkConf = sparkContext.getConf
  val numaBindingInfo = GlutenConfig.getConf.numaBindingInfo
  val substraitPlanLogLevel = GlutenConfig.getConf.substraitPlanLogLevel

  private var planJson: String = ""

  def getPlanJson: String = {
    if (planJson.isEmpty) {
      logWarning("Plan in JSON string is empty. This may due to the plan has not been executed.")
    }
    planJson
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def supportsColumnar: Boolean = GlutenConfig.getConf.enableColumnarIterator

  override def otherCopyArgs: Seq[AnyRef] = Seq(transformStageId.asInstanceOf[Integer])

  override def generateTreeString(
      depth: Int,
      lastChildren: Seq[Boolean],
      append: String => Unit,
      verbose: Boolean,
      prefix: String = "",
      addSuffix: Boolean = false,
      maxFields: Int,
      printNodeId: Boolean,
      indent: Int = 0): Unit = {
    val prefix = if (printNodeId) "^ " else s"^($transformStageId) "
    child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      prefix,
      false,
      maxFields,
      printNodeId,
      indent)
    if (verbose && planJson.nonEmpty) {
      append(prefix + "Substrait plan:\n")
      append(planJson)
      append("\n")
    }
  }

  override def nodeName: String = s"WholeStageCodegenTransformer ($transformStageId)"

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {
    child.asInstanceOf[TransformSupport].getBuildPlans
  }

  override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException("Row based execution is not supported")
  }

  def doWholestageTransform(): WholestageTransformContext = {
    val substraitContext = new SubstraitContext
    val childCtx = child
      .asInstanceOf[TransformSupport]
      .doTransform(substraitContext)
    if (childCtx == null) {
      throw new NullPointerException(s"ColumnarWholestageTransformer can't doTansform on $child")
    }
    val outNames = new java.util.ArrayList[String]()
    for (attr <- childCtx.outputAttributes) {
      outNames.add(ConverterUtils.genColumnNameWithExprId(attr))
    }
    val planNode =
      PlanBuilder.makePlan(substraitContext, Lists.newArrayList(childCtx.root), outNames)

    planJson = SubstraitPlanPrinterUtil.substraitPlanToJson(planNode.toProtobuf)

    WholestageTransformContext(
      childCtx.inputAttributes,
      childCtx.outputAttributes,
      planNode,
      substraitContext)
  }

  /** Find all BasicScanExecTransformers in one WholeStageTransformerExec */
  def checkBatchScanExecTransformerChildren(): Seq[BasicScanExecTransformer] = {
    val basicScanExecTransformers = new mutable.ListBuffer[BasicScanExecTransformer]()

    def transformChildren(
        plan: SparkPlan,
        basicScanExecTransformers: mutable.ListBuffer[BasicScanExecTransformer]): Unit = {
      if (plan != null && plan.isInstanceOf[TransformSupport]) {
        plan match {
          case transformer: BasicScanExecTransformer =>
            basicScanExecTransformers.append(transformer)
          case _ =>
        }
        // according to the substrait plan order
        plan match {
          case shj: HashJoinLikeExecTransformer =>
            transformChildren(shj.streamedPlan, basicScanExecTransformers)
            transformChildren(shj.buildPlan, basicScanExecTransformers)
          case t: TransformSupport =>
            t.children
              .foreach(transformChildren(_, basicScanExecTransformers))
        }
      }
    }

    transformChildren(child, basicScanExecTransformers)
    basicScanExecTransformers.toSeq
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    TestStats.offloadGluten = true
    val pipelineTime: SQLMetric = longMetric("pipelineTime")

    val buildRelationBatchHolder: mutable.ListBuffer[ColumnarBatch] = mutable.ListBuffer()

    val inputRDDs = columnarInputRDDs
    // Check if BatchScan exists.
    val basicScanExecTransformers = checkBatchScanExecTransformerChildren()

    if (basicScanExecTransformers.nonEmpty) {

      /**
       * If containing scan exec transformer this "whole stage" generates a RDD which itself takes
       * care of SCAN there won't be any other RDD for SCAN as a result, genFirstStageIterator
       * rather than genFinalStageIterator will be invoked
       */
      // the partition size of the all BasicScanExecTransformer must be the same
      val allScanPartitions = basicScanExecTransformers.map(_.getFlattenPartitions)
      val partitionLength = allScanPartitions.head.size
      if (allScanPartitions.exists(_.size != partitionLength)) {
        throw new RuntimeException(
          "The partition length of all the scan transformer are not the same.")
      }
      val startTime = System.nanoTime()
      val wsCxt = doWholestageTransform()

      // the file format for each scan exec
      wsCxt.substraitContext.setFileFormat(
        basicScanExecTransformers.map(ConverterUtils.getFileFormat).asJava)

      // generate each partition of all scan exec
      val substraitPlanPartitions = (0 until partitionLength).map(
        i => {
          val currentPartitions = allScanPartitions.map(_(i))
          BackendsApiManager.getIteratorApiInstance
            .genFilePartition(i, currentPartitions, wsCxt)
        })

      logOnLevel(
        substraitPlanLogLevel,
        s"Generating the Substrait plan took: ${(System.nanoTime() - startTime)} ns.")

      new GlutenWholeStageColumnarRDD(
        sparkContext,
        substraitPlanPartitions,
        wsCxt.outputAttributes,
        genFirstNewRDDsForBroadcast(inputRDDs, partitionLength),
        pipelineTime,
        leafMetricsUpdater().updateInputMetrics,
        BackendsApiManager.getMetricsApiInstance.metricsUpdatingFunction(
          child,
          wsCxt.substraitContext.registeredRelMap,
          wsCxt.substraitContext.registeredJoinParams,
          wsCxt.substraitContext.registeredAggregationParams
        )
      )
    } else {

      /**
       * the whole stage contains NO BasicScanExecTransformer. this the default case for:
       *   1. SCAN with clickhouse backend (check ColumnarCollapseCodegenStages#separateScanRDD())
       *      2. test case where query plan is constructed from simple dataframes (e.g.
       *      GlutenDataFrameAggregateSuite) in these cases, separate RDDs takes care of SCAN as a
       *      result, genFinalStageIterator rather than genFirstStageIterator will be invoked
       */
      val startTime = System.nanoTime()
      val resCtx = doWholestageTransform()

      logOnLevel(substraitPlanLogLevel, s"Generating substrait plan:\n${planJson}")
      logOnLevel(
        substraitPlanLogLevel,
        s"Generating the Substrait plan took: ${(System.nanoTime() - startTime)} ns.")

      new WholeStageZippedPartitionsRDD(
        sparkContext,
        genFinalNewRDDsForBroadcast(inputRDDs),
        numaBindingInfo,
        sparkConf,
        resCtx,
        pipelineTime,
        buildRelationBatchHolder,
        BackendsApiManager.getMetricsApiInstance.metricsUpdatingFunction(
          child,
          resCtx.substraitContext.registeredRelMap,
          resCtx.substraitContext.registeredJoinParams,
          resCtx.substraitContext.registeredAggregationParams
        )
      )
    }
  }

  override def getStreamedLeafPlan: SparkPlan = {
    child.asInstanceOf[TransformSupport].getStreamedLeafPlan
  }

  override def metricsUpdater(): MetricsUpdater = {
    child match {
      case transformer: TransformSupport => transformer.metricsUpdater()
      case _ => new NoopMetricsUpdater
    }
  }

  def leafMetricsUpdater(): MetricsUpdater = {
    getStreamedLeafPlan match {
      case transformer: TransformSupport => transformer.metricsUpdater()
      case _ => new NoopMetricsUpdater
    }
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = child match {
    case c: TransformSupport =>
      c.columnarInputRDDs
    case _ =>
      throw new UnsupportedOperationException
  }

  // Recreate the broadcast build side rdd with matched partition number.
  // Used when whole stage transformer contains scan.
  def genFirstNewRDDsForBroadcast(
      rddSeq: Seq[RDD[ColumnarBatch]],
      partitions: Int): Seq[RDD[ColumnarBatch]] = {
    rddSeq.map {
      case rdd: BroadcastBuildSideRDD =>
        rdd.copy(numPartitions = partitions)
      case inputRDD =>
        inputRDD
    }
  }

  // Recreate the broadcast build side rdd with matched partition number.
  // Used when whole stage transformer does not contain scan.
  def genFinalNewRDDsForBroadcast(rddSeq: Seq[RDD[ColumnarBatch]]): Seq[RDD[ColumnarBatch]] = {
    // Get the number of partitions from a non-broadcast RDD.
    val nonBroadcastRDD = rddSeq.find(rdd => !rdd.isInstanceOf[BroadcastBuildSideRDD])
    if (nonBroadcastRDD.isEmpty) {
      throw new RuntimeException("At least one RDD should not being BroadcastBuildSideRDD")
    }
    rddSeq.map {
      case broadcastRDD: BroadcastBuildSideRDD =>
        try {
          broadcastRDD.getNumPartitions
          broadcastRDD
        } catch {
          case _: Throwable =>
            // Recreate the broadcast build side rdd with matched partition number.
            broadcastRDD.copy(numPartitions = nonBroadcastRDD.orNull.getNumPartitions)
        }
      case rdd =>
        rdd
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WholeStageTransformerExec =
    copy(child = newChild)(transformStageId)
}
