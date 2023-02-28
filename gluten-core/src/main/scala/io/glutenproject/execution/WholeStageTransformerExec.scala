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

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.collect.Lists
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression._
import io.glutenproject.substrait.{AggregationParams, JoinParams, SubstraitContext}
import io.glutenproject.substrait.plan.{PlanBuilder, PlanNode}
import io.glutenproject.substrait.rel.RelNode
import io.glutenproject.test.TestStats
import io.glutenproject.utils.{LogLevelUtil, SubstraitPlanPrinterUtil}
import io.glutenproject.vectorized._

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class TransformContext(
    inputAttributes: Seq[Attribute],
    outputAttributes: Seq[Attribute],
    root: RelNode)

case class WholestageTransformContext(
    inputAttributes: Seq[Attribute],
    outputAttributes: Seq[Attribute],
    root: PlanNode,
    substraitContext: SubstraitContext = null)

/**
 * A minimized controller for updating operator's metrics, which means it never
 * persists the SparkPlan instance of the operator then the serialized RDD's size
 * can be therefore minimized.
 *
 * TODO place it to some other where since it's used not only by whole stage facilities
 */
trait MetricsUpdater extends Serializable {
  def updateOutputMetrics(outNumBatches: Long, outNumRows: Long): Unit = {}
  def updateNativeMetrics(operatorMetrics: OperatorMetrics): Unit = {}
}

/**
 * A tree to walk down the operators' MetricsUpdater instances.
 */
final case class MetricsUpdaterTree(updater: MetricsUpdater, children: Seq[MetricsUpdaterTree])

object NoopMetricsUpdater extends MetricsUpdater

trait TransformSupport extends SparkPlan {

  /**
   * Validate whether this SparkPlan supports to be transformed into substrait node in Native Code.
   */
  def doValidate(): Boolean = false

  /**
   * Returns all the RDDs of ColumnarBatch which generates the input rows.
   *
   * @note
   *   Right now we support up to two RDDs
   */
  def columnarInputRDDs: Seq[RDD[ColumnarBatch]]

  def getBuildPlans: Seq[(SparkPlan, SparkPlan)]

  def getStreamedLeafPlan: SparkPlan

  def getChild: SparkPlan

  def doTransform(context: SubstraitContext): TransformContext = {
    throw new UnsupportedOperationException(
      s"This operator doesn't support doTransform with SubstraitContext.")
  }

  def metricsUpdater(): MetricsUpdater

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
  override lazy val metrics = Map(
    "pipelineTime" -> SQLMetrics.createTimingMetric(sparkContext, "duration"))
  val sparkConf = sparkContext.getConf
  val numaBindingInfo = GlutenConfig.getConf.numaBindingInfo
  val substraitPlanLogLevel = GlutenConfig.getSessionConf.substraitPlanLogLevel

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
    val prefix = if (printNodeId) "* " else s"*($transformStageId) "
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

  override def getChild: SparkPlan = child

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

  @deprecated
  def checkBatchScanExecTransformerChild(): Option[BasicScanExecTransformer] = {
    var currentOp = child
    while (
      currentOp.isInstanceOf[TransformSupport] &&
      !currentOp.isInstanceOf[BasicScanExecTransformer] &&
      currentOp.asInstanceOf[TransformSupport].getChild != null
    ) {
      currentOp = currentOp.asInstanceOf[TransformSupport].getChild
    }
    if (
      currentOp != null &&
      currentOp.isInstanceOf[BasicScanExecTransformer]
    ) {
      currentOp match {
        case op: BatchScanExecTransformer =>
          Some(currentOp.asInstanceOf[BatchScanExecTransformer])
        case op: FileSourceScanExecTransformer =>
          Some(currentOp.asInstanceOf[FileSourceScanExecTransformer])
      }
    } else {
      None
    }
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

      val metricsUpdatingFunction: Metrics => Unit =
        updateNativeMetrics(
          wsCxt.substraitContext.registeredRelMap,
          wsCxt.substraitContext.registeredJoinParams,
          wsCxt.substraitContext.registeredAggregationParams)

      new GlutenWholeStageColumnarRDD(
        sparkContext,
        substraitPlanPartitions,
        wsCxt.outputAttributes,
        genFirstNewRDDsForBroadcast(inputRDDs, partitionLength),
        pipelineTime,
        metricsUpdater().updateOutputMetrics,
        metricsUpdatingFunction
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

      val metricsUpdatingFunction: Metrics => Unit =
        updateNativeMetrics(
          resCtx.substraitContext.registeredRelMap,
          resCtx.substraitContext.registeredJoinParams,
          resCtx.substraitContext.registeredAggregationParams)

      new WholeStageZippedPartitionsRDD(
        sparkContext,
        genFinalNewRDDsForBroadcast(inputRDDs),
        numaBindingInfo,
        sparkConf,
        resCtx,
        pipelineTime,
        buildRelationBatchHolder,
        metricsUpdater().updateOutputMetrics,
        metricsUpdatingFunction)
    }
  }

  override def getStreamedLeafPlan: SparkPlan = {
    child.asInstanceOf[TransformSupport].getStreamedLeafPlan
  }

  override def metricsUpdater(): MetricsUpdater = {
    child match {
      case transformer: TransformSupport => transformer.metricsUpdater()
      case _ => NoopMetricsUpdater
    }
  }

  /**
   * Update metrics fetched from certain iterator to transformers.
   * @param metrics
   *   the metrics recorded by transformer's operators
   * @param relMap
   *   the map between operator index and its rels
   * @param joinParamsMap
   *   the map between operator index and join parameters
   * @param aggParamsMap
   *   the map between operator index and aggregation parameters
   */
  def updateNativeMetrics(
      relMap: java.util.HashMap[java.lang.Long, java.util.ArrayList[java.lang.Long]],
      joinParamsMap: java.util.HashMap[java.lang.Long, JoinParams],
      aggParamsMap: java.util.HashMap[java.lang.Long, AggregationParams])
  : Metrics => Unit = {
    def treeifyMetricsUpdaters(plan: SparkPlan): MetricsUpdaterTree = {
      plan match {
        case j: HashJoinLikeExecTransformer =>
          MetricsUpdaterTree(j.metricsUpdater(),
            Seq(treeifyMetricsUpdaters(j.buildPlan), treeifyMetricsUpdaters(j.streamedPlan)))
        case t: TransformSupport =>
          MetricsUpdaterTree(t.metricsUpdater(), t.children.map(treeifyMetricsUpdaters))
        case _ =>
          MetricsUpdaterTree(NoopMetricsUpdater, Seq())
      }
    }
    val mut: MetricsUpdaterTree = treeifyMetricsUpdaters(child)

    WholeStageTransformerExec.updateTransformerMetrics(
      mut,
      relMap,
      new java.lang.Long(relMap.size() - 1),
      joinParamsMap,
      aggParamsMap)
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

object WholeStageTransformerExec extends Logging {

  /**
   * Merge several suites of metrics together.
   * @param operatorMetrics:
   *   a list of metrics to merge
   * @return
   *   the merged metrics
   */
  private def mergeMetrics(
      operatorMetrics: java.util.ArrayList[OperatorMetrics]): OperatorMetrics = {
    if (operatorMetrics.size() == 0) {
      return null
    }

    // We are accessing the metrics from end to start. So the input metrics are got from the
    // last suite of metrics, and the output metrics are got from the first suite.
    val inputRows = operatorMetrics.get(operatorMetrics.size() - 1).inputRows
    val inputVectors = operatorMetrics.get(operatorMetrics.size() - 1).inputVectors
    val inputBytes = operatorMetrics.get(operatorMetrics.size() - 1).inputBytes
    val rawInputRows = operatorMetrics.get(operatorMetrics.size() - 1).rawInputRows
    val rawInputBytes = operatorMetrics.get(operatorMetrics.size() - 1).rawInputBytes

    val outputRows = operatorMetrics.get(0).outputRows
    val outputVectors = operatorMetrics.get(0).outputVectors
    val outputBytes = operatorMetrics.get(0).outputBytes

    var cpuCount: Long = 0
    var wallNanos: Long = 0
    var peakMemoryBytes: Long = 0
    var numMemoryAllocations: Long = 0
    var spilledBytes: Long = 0
    var spilledRows: Long = 0
    var spilledPartitions: Long = 0
    var spilledFiles: Long = 0
    var numDynamicFiltersProduced: Long = 0
    var numDynamicFiltersAccepted: Long = 0
    var numReplacedWithDynamicFilterRows: Long = 0
    var flushRowCount: Long = 0
    var scanTime: Long = 0

    val metricsIterator = operatorMetrics.iterator()
    while (metricsIterator.hasNext) {
      val metrics = metricsIterator.next()
      cpuCount += metrics.cpuCount
      wallNanos += metrics.wallNanos
      peakMemoryBytes = peakMemoryBytes.max(metrics.peakMemoryBytes)
      numMemoryAllocations += metrics.numMemoryAllocations
      spilledBytes += metrics.spilledBytes
      spilledRows += metrics.spilledRows
      spilledPartitions += metrics.spilledPartitions
      spilledFiles += metrics.spilledFiles
      numDynamicFiltersProduced += metrics.numDynamicFiltersProduced
      numDynamicFiltersAccepted += metrics.numDynamicFiltersAccepted
      numReplacedWithDynamicFilterRows += metrics.numReplacedWithDynamicFilterRows
      flushRowCount += metrics.flushRowCount
      scanTime += metrics.scanTime
    }

    new OperatorMetrics(
      inputRows,
      inputVectors,
      inputBytes,
      rawInputRows,
      rawInputBytes,
      outputRows,
      outputVectors,
      outputBytes,
      cpuCount,
      wallNanos,
      peakMemoryBytes,
      numMemoryAllocations,
      spilledBytes,
      spilledRows,
      spilledPartitions,
      spilledFiles,
      numDynamicFiltersProduced,
      numDynamicFiltersAccepted,
      numReplacedWithDynamicFilterRows,
      flushRowCount,
      scanTime
    )
  }

  /**
   * @return
   *   operator index and metrics index
   */
  def updateTransformerMetricsInternal(
      mutNode: MetricsUpdaterTree,
      relMap: java.util.HashMap[java.lang.Long, java.util.ArrayList[java.lang.Long]],
      operatorIdx: java.lang.Long,
      metrics: Metrics,
      metricsIdx: Int,
      joinParamsMap: java.util.HashMap[java.lang.Long, JoinParams],
      aggParamsMap: java.util.HashMap[java.lang.Long, AggregationParams]): (java.lang.Long, Int) = {
    val operatorMetrics = new java.util.ArrayList[OperatorMetrics]()
    var curMetricsIdx = metricsIdx
    relMap
      .get(operatorIdx)
      .forEach(
        _ => {
          operatorMetrics.add(metrics.getOperatorMetrics(curMetricsIdx))
          curMetricsIdx -= 1
        })

    mutNode.updater match {
      case ju: HashJoinMetricsUpdater =>
        // JoinRel outputs two suites of metrics respectively for hash build and hash probe.
        // Therefore, fetch one more suite of metrics here.
        operatorMetrics.add(metrics.getOperatorMetrics(curMetricsIdx))
        curMetricsIdx -= 1
        ju.updateJoinMetrics(
          operatorMetrics,
          metrics.getSingleMetrics,
          joinParamsMap.get(operatorIdx))
      case hau: HashAggregateMetricsUpdater =>
        hau.updateAggregationMetrics(operatorMetrics, aggParamsMap.get(operatorIdx))
      case lu: LimitMetricsUpdater =>
        // Limit over Sort is converted to TopN node in Velox, so there is only one suite of metrics
        // for the two transformers. We do not update metrics for limit and leave it for sort.
        if (!mutNode.children.head.updater.isInstanceOf[SortMetricsUpdater]) {
          val opMetrics: OperatorMetrics = mergeMetrics(operatorMetrics)
          lu.updateNativeMetrics(opMetrics)
        }
      case u =>
        val opMetrics: OperatorMetrics = mergeMetrics(operatorMetrics)
        u.updateNativeMetrics(opMetrics)
    }

    var newOperatorIdx: java.lang.Long = operatorIdx - 1
    var newMetricsIdx: Int = if (mutNode.updater.isInstanceOf[LimitMetricsUpdater] &&
      mutNode.children.head.updater.isInstanceOf[SortMetricsUpdater]) {
      // This suite of metrics is not consumed.
      metricsIdx
    } else {
      curMetricsIdx
    }

    mutNode.children.foreach { child =>
      if (child.updater ne NoopMetricsUpdater) {
        val result = updateTransformerMetricsInternal(
          child,
          relMap,
          newOperatorIdx,
          metrics,
          newMetricsIdx,
          joinParamsMap,
          aggParamsMap)
        newOperatorIdx = result._1
        newMetricsIdx = result._2
      }
    }

    (newOperatorIdx, newMetricsIdx)
  }

  /**
   * A recursive function updating the metrics of one transformer and its child.
   * @param mut
   *   the metrics updater tree built from the original plan
   * @param relMap
   *   the map between operator index and its rels
   * @param operatorIdx
   *   the index of operator
   * @param metrics
   *   the metrics fetched from native
   * @param metricsIdx
   *   the index of metrics
   * @param joinParamsMap
   *   the map between operator index and join parameters
   * @param aggParamsMap
   *   the map between operator index and aggregation parameters
   */
  def updateTransformerMetrics(
      mutNode: MetricsUpdaterTree,
      relMap: java.util.HashMap[java.lang.Long, java.util.ArrayList[java.lang.Long]],
      operatorIdx: java.lang.Long,
      joinParamsMap: java.util.HashMap[java.lang.Long, JoinParams],
      aggParamsMap: java.util.HashMap[java.lang.Long, AggregationParams])
  : Metrics => Unit = { metrics =>
    try {
      val numNativeMetrics = metrics.inputRows.length
      if (numNativeMetrics == 0) {
        ()
      } else if (mutNode.updater eq NoopMetricsUpdater) {
        ()
      } else {
        updateTransformerMetricsInternal(
          mutNode,
          relMap,
          operatorIdx,
          metrics,
          numNativeMetrics - 1,
          joinParamsMap,
          aggParamsMap)
      }
    } catch {
      case e: Throwable =>
        logWarning(s"Updating native metrics failed due to ${e.getCause}.")
        ()
    }
  }
}
