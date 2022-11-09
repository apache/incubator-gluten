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
import scala.collection.JavaConverters._
import scala.collection.mutable

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression._
import io.glutenproject.substrait.{AggregationParams, JoinParams, SubstraitContext}
import io.glutenproject.substrait.plan.{PlanBuilder, PlanNode}
import io.glutenproject.substrait.rel.RelNode
import io.glutenproject.test.TestStats
import io.glutenproject.vectorized._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

case class TransformContext(inputAttributes: Seq[Attribute],
                            outputAttributes: Seq[Attribute],
                            root: RelNode)

case class WholestageTransformContext(inputAttributes: Seq[Attribute],
                                      outputAttributes: Seq[Attribute],
                                      root: PlanNode,
                                      substraitContext: SubstraitContext = null)

trait TransformSupport extends SparkPlan {

  /**
   * Validate whether this SparkPlan supports to be transformed into substrait node in Native Code.
   */
  def doValidate(): Boolean = false

  /**
   * Returns all the RDDs of ColumnarBatch which generates the input rows.
   *
   * @note Right now we support up to two RDDs
   */
  def columnarInputRDDs: Seq[RDD[ColumnarBatch]]

  def getBuildPlans: Seq[(SparkPlan, SparkPlan)]

  def getStreamedLeafPlan: SparkPlan

  def getChild: SparkPlan

  def doTransform(context: SubstraitContext): TransformContext = {
    throw new UnsupportedOperationException(
      s"This operator doesn't support doTransform with SubstraitContext.")
  }

  def dependentPlanCtx: TransformContext = null

  def updateMetrics(outNumBatches: Long, outNumRows: Long): Unit = {}

  def updateNativeMetrics(operatorMetrics: OperatorMetrics): Unit = {}

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
  extends UnaryExecNode
    with TransformSupport {

  // For WholeStageCodegen-like operator, only pipeline time will be handled in graph plotting.
  // See SparkPlanGraph.scala:205 for reference.
  override lazy val metrics = Map(
    "pipelineTime" -> SQLMetrics.createTimingMetric(sparkContext, "duration"))
  val sparkConf = sparkContext.getConf
  val numaBindingInfo = GlutenConfig.getConf.numaBindingInfo

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def supportsColumnar: Boolean = GlutenConfig.getConf.enableColumnarIterator

  override def otherCopyArgs: Seq[AnyRef] = Seq(transformStageId.asInstanceOf[Integer])

  override def generateTreeString(depth: Int,
                                  lastChildren: Seq[Boolean],
                                  append: String => Unit,
                                  verbose: Boolean,
                                  prefix: String = "",
                                  addSuffix: Boolean = false,
                                  maxFields: Int,
                                  printNodeId: Boolean,
                                  indent: Int = 0): Unit = {
    val res = child.generateTreeString(
      depth,
      lastChildren,
      append,
      verbose,
      if (printNodeId) "* " else s"*($transformStageId) ",
      false,
      maxFields,
      printNodeId,
      indent)
    res
  }

  override def nodeName: String = s"WholeStageCodegenTransformer (${transformStageId})"

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {
    child.asInstanceOf[TransformSupport].getBuildPlans
  }

  override def getChild: SparkPlan = child

  @deprecated
  override def doExecute(): RDD[InternalRow] = {
    // check if BatchScan exists
    val currentOp = checkBatchScanExecTransformerChild()
    if (currentOp.isDefined) {
      // If containing scan exec transformer, a new RDD is created.
      val fileScan = currentOp.get
      val wsCxt = doWholestageTransform()

      val startTime = System.nanoTime()
      val substraitPlanPartition = fileScan.getFlattenPartitions.map(p =>
        BackendsApiManager.getIteratorApiInstance.genNativeFilePartition(-1, null, wsCxt))
      logDebug(
        s"Generated substrait plan tooks: ${(System.nanoTime() - startTime) / 1000000} ms")

      val wsRDD = new NativeWholestageRowRDD(sparkContext, substraitPlanPartition, false)
      wsRDD
    } else {
      sparkContext.emptyRDD
    }
  }

  def doWholestageTransform(): WholestageTransformContext = {
    val substraitContext = new SubstraitContext
    val childCtx = child
      .asInstanceOf[TransformSupport]
      .doTransform(substraitContext)
    if (childCtx == null) {
      throw new NullPointerException(
        s"ColumnarWholestageTransformer can't doTansform on ${child}")
    }
    val outNames = new java.util.ArrayList[String]()
    for (attr <- childCtx.outputAttributes) {
      outNames.add(ConverterUtils.genColumnNameWithExprId(attr))
    }
    val planNode =
      PlanBuilder.makePlan(substraitContext, Lists.newArrayList(childCtx.root), outNames)

    WholestageTransformContext(
      childCtx.inputAttributes,
      childCtx.outputAttributes,
      planNode,
      substraitContext)
  }

  @deprecated
  def checkBatchScanExecTransformerChild(): Option[BasicScanExecTransformer] = {
    var currentOp = child
    while (currentOp.isInstanceOf[TransformSupport] &&
      !currentOp.isInstanceOf[BasicScanExecTransformer] &&
      currentOp.asInstanceOf[TransformSupport].getChild != null) {
      currentOp = currentOp.asInstanceOf[TransformSupport].getChild
    }
    if (currentOp != null &&
      currentOp.isInstanceOf[BasicScanExecTransformer]) {
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

  /**
   * Find all BasicScanExecTransformers in one WholeStageTransformerExec
   */
  def checkBatchScanExecTransformerChildren(): Seq[BasicScanExecTransformer] = {
    val basicScanExecTransformers = new mutable.ListBuffer[BasicScanExecTransformer]()

    def transformChildren(plan: SparkPlan,
                          basicScanExecTransformers: mutable.ListBuffer[BasicScanExecTransformer]
                         ): Unit = {
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
          case _ =>
            plan.asInstanceOf[TransformSupport].children.foreach(transformChildren(_,
              basicScanExecTransformers))
        }
      }
    }

    transformChildren(child, basicScanExecTransformers)
    basicScanExecTransformers.toSeq.map( scan => {
      scan match {
        case op: BatchScanExecTransformer =>
          op.asInstanceOf[BatchScanExecTransformer]
        case op: FileSourceScanExecTransformer =>
          op.asInstanceOf[FileSourceScanExecTransformer]
      }
    })
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    TestStats.offloadGluten = true
    val pipelineTime: SQLMetric = longMetric("pipelineTime")

    val signature = doBuild()
    val listJars = uploadAndListJars(signature)

    // we should zip all dependent RDDs to current main RDD
    // TODO: Does it still need these parameters?
    val dependentKernels: mutable.ListBuffer[ExpressionEvaluator] = mutable.ListBuffer()
    val dependentKernelIterators: mutable.ListBuffer[GeneralOutIterator] = mutable.ListBuffer()
    val buildRelationBatchHolder: mutable.ListBuffer[ColumnarBatch] = mutable.ListBuffer()

    val inputRDDs = columnarInputRDDs
    // Check if BatchScan exists.
    val basicScanExecTransformer = checkBatchScanExecTransformerChildren()

    // If containing scan exec transformer, a new RDD is created.
    if (basicScanExecTransformer.nonEmpty) {
      // the partition size of the all BasicScanExecTransformer must be the same
      val allScanPartitions = basicScanExecTransformer.map(_.getFlattenPartitions)
      val partitionLength = allScanPartitions(0).size
      if (allScanPartitions.exists(_.size != partitionLength)) {
        throw new RuntimeException(
          "The partition length of all the scan transformer are not the same.")
      }
      val startTime = System.nanoTime()
      val wsCxt = doWholestageTransform()

      // the file format for each scan exec
      wsCxt.substraitContext.setFileFormat(
        basicScanExecTransformer.map(ConverterUtils.getFileFormat).asJava)

      // generate each partition of all scan exec
      val substraitPlanPartition = (0 until partitionLength).map( i => {
        val currentPartitions = allScanPartitions.map(_(i))
        BackendsApiManager.getIteratorApiInstance.genNativeFilePartition(
          i, currentPartitions, wsCxt)
      })

      logInfo(
        s"Generating the Substrait plan took: ${(System.nanoTime() - startTime)} ns.")

      val metricsUpdatingFunction: GeneralOutIterator => Unit = (resIter: GeneralOutIterator) =>
        updateNativeMetrics(
          resIter,
          wsCxt.substraitContext.registeredRelMap,
          wsCxt.substraitContext.registeredJoinParams,
          wsCxt.substraitContext.registeredAggregationParams)

      new NativeWholeStageColumnarRDD(
        sparkContext,
        substraitPlanPartition,
        wsCxt.outputAttributes,
        genFirstNewRDDsForBroadcast(inputRDDs, partitionLength),
        pipelineTime,
        updateMetrics,
        metricsUpdatingFunction)
    } else {
      val startTime = System.nanoTime()
      val resCtx = doWholestageTransform()
      logInfo(
        s"Generating the Substrait plan took: ${(System.nanoTime() - startTime)} ns.")
      logDebug(s"Generating substrait plan:\n${resCtx.root.toProtobuf.toString}")

      val metricsUpdatingFunction: GeneralOutIterator => Unit = (resIter: GeneralOutIterator) =>
        updateNativeMetrics(
          resIter,
          resCtx.substraitContext.registeredRelMap,
          resCtx.substraitContext.registeredJoinParams,
          resCtx.substraitContext.registeredAggregationParams)

      val genFinalStageIterator = (inputIterators: Seq[Iterator[ColumnarBatch]]) => {
        BackendsApiManager.getIteratorApiInstance
          .genFinalStageIterator(
            inputIterators,
            numaBindingInfo,
            listJars,
            signature,
            sparkConf,
            resCtx.outputAttributes,
            resCtx.root,
            pipelineTime,
            updateMetrics,
            metricsUpdatingFunction,
            buildRelationBatchHolder,
            dependentKernels,
            dependentKernelIterators)
      }

      new WholeStageZippedPartitionsRDD(
        sparkContext, genFinalNewRDDsForBroadcast(inputRDDs), genFinalStageIterator)
    }
  }

  def uploadAndListJars(signature: String): Seq[String] =
    if (signature != "") {
      if (sparkContext.listJars.filter(path => path.contains(s"${signature}.jar")).isEmpty) {
        val tempDir = GlutenConfig.getRandomTempDir
        val jarFileName =
          s"${tempDir}/tmp/spark-columnar-plugin-codegen-precompile-${signature}.jar"
        sparkContext.addJar(jarFileName)
      }
      sparkContext.listJars.filter(path => path.contains(s"${signature}.jar"))
    } else {
      Seq()
    }

  override def getStreamedLeafPlan: SparkPlan = {
    child.asInstanceOf[TransformSupport].getStreamedLeafPlan
  }

  /**
   * Update metrics for the child plan.
   *
   * @param outNumBatches the number of batches to add
   * @param outNumRows    the number of rows to add
   */
  override def updateMetrics(outNumBatches: Long, outNumRows: Long): Unit = {
    // Update output batches and rows to the last child.
    child match {
      case transformer: TransformSupport =>
        transformer.updateMetrics(outNumBatches, outNumRows)
      case _ =>
    }
  }

  /**
   * Merge several suites of metrics together.
   * @param operatorMetrics: a list of metrics to merge
   * @return the merged metrics
   */
  private def mergeMetrics(operatorMetrics: java.util.ArrayList[OperatorMetrics])
    : OperatorMetrics = {
    if (operatorMetrics.size() == 0) {
      return null
    }

    // We are accessing the metrics from end to start. So the input metrics are got from the
    // last suite of metrics, and the output metrics are got from the first suite.
    val inputRows = operatorMetrics.get(operatorMetrics.size() -1).inputRows
    val inputVectors = operatorMetrics.get(operatorMetrics.size() -1).inputVectors
    val inputBytes = operatorMetrics.get(operatorMetrics.size() -1).inputBytes
    val rawInputRows = operatorMetrics.get(operatorMetrics.size() -1).rawInputRows
    val rawInputBytes = operatorMetrics.get(operatorMetrics.size() -1).rawInputBytes

    val outputRows = operatorMetrics.get(0).outputRows
    val outputVectors = operatorMetrics.get(0).outputVectors
    val outputBytes = operatorMetrics.get(0).outputBytes

    var count: Long = 0
    var wallNanos: Long = 0
    var peakMemoryBytes: Long = 0
    var numMemoryAllocations: Long = 0
    var numDynamicFiltersProduced: Long = 0
    var numDynamicFiltersAccepted: Long = 0
    var numReplacedWithDynamicFilterRows: Long = 0

    val metricsIterator = operatorMetrics.iterator()
    while (metricsIterator.hasNext) {
      val metrics = metricsIterator.next()
      count += metrics.count
      wallNanos += metrics.wallNanos
      peakMemoryBytes = peakMemoryBytes.max(metrics.peakMemoryBytes)
      numMemoryAllocations += metrics.numMemoryAllocations
      numDynamicFiltersProduced += metrics.numDynamicFiltersProduced
      numDynamicFiltersAccepted += metrics.numDynamicFiltersAccepted
      numReplacedWithDynamicFilterRows += metrics.numReplacedWithDynamicFilterRows
    }

    new OperatorMetrics(
      inputRows, inputVectors, inputBytes, rawInputRows, rawInputBytes, outputRows, outputVectors,
      outputBytes, count, wallNanos, peakMemoryBytes, numMemoryAllocations,
      numDynamicFiltersProduced, numDynamicFiltersAccepted, numReplacedWithDynamicFilterRows)
  }


  /**
   * A recursive function updating the metrics of one transformer and its child.
   * @param curChild the transformer to update metrics to
   * @param relMap the map between operator index and its rels
   * @param operatorIdx the index of operator
   * @param metrics the metrics fetched from native
   * @param metricsIdx the index of metrics
   * @param joinParamsMap the map between operator index and join parameters
   * @param aggParamsMap the map between operator index and aggregation parameters
   * @return operator index and metrics index
   */
  def updateTransformerMetrics(
      curChild: TransformSupport,
      relMap: java.util.HashMap[java.lang.Long, java.util.ArrayList[java.lang.Long]],
      operatorIdx: java.lang.Long,
      metrics: Metrics,
      metricsIdx: Int,
      joinParamsMap: java.util.HashMap[java.lang.Long, JoinParams],
      aggParamsMap: java.util.HashMap[java.lang.Long, AggregationParams]): (java.lang.Long, Int) = {
    val operatorMetrics = new java.util.ArrayList[OperatorMetrics]()
    var curMetricsIdx = metricsIdx
    relMap.get(operatorIdx).forEach(_ => {
      operatorMetrics.add(metrics.getOperatorMetrics(curMetricsIdx))
      curMetricsIdx -= 1
    })

    curChild match {
      case joinTransformer: HashJoinLikeExecTransformer =>
        // JoinRel outputs two suites of metrics respectively for hash build and hash probe.
        // Therefore, fetch one more suite of metrics here.
        operatorMetrics.add(metrics.getOperatorMetrics(curMetricsIdx))
        curMetricsIdx -= 1
        joinTransformer.updateJoinMetrics(operatorMetrics, metrics.getSingleMetrics,
          joinParamsMap.get(operatorIdx))

        var newOperatorIdx: java.lang.Long = operatorIdx - 1
        var newMetricsIdx: Int = curMetricsIdx
        joinTransformer.buildPlan match {
          case transformer: TransformSupport =>
            val result = updateTransformerMetrics(transformer, relMap, newOperatorIdx,
              metrics, newMetricsIdx, joinParamsMap, aggParamsMap)
            newOperatorIdx = result._1
            newMetricsIdx = result._2
          case _ =>
        }

        joinTransformer.streamedPlan match {
          case transformer: TransformSupport =>
            val result = updateTransformerMetrics(transformer, relMap, newOperatorIdx,
              metrics, newMetricsIdx, joinParamsMap, aggParamsMap)
            newOperatorIdx = result._1
            newMetricsIdx = result._2
          case _ =>
        }
        (newOperatorIdx, newMetricsIdx)
      case aggTransformer: HashAggregateExecBaseTransformer =>
        aggTransformer.updateAggregationMetrics(
          operatorMetrics, aggParamsMap.get(operatorIdx))

        var newOperatorIdx: java.lang.Long = operatorIdx - 1
        var newMetricsIdx: Int = curMetricsIdx

        if (curChild.getChild == null) {
          return (newOperatorIdx, newMetricsIdx)
        }
        curChild.getChild match {
          case transformer: TransformSupport =>
            val result = updateTransformerMetrics(transformer, relMap, newOperatorIdx,
              metrics, newMetricsIdx, joinParamsMap, aggParamsMap)
            newOperatorIdx = result._1
            newMetricsIdx = result._2
          case _ =>
        }
        (newOperatorIdx, newMetricsIdx)
      case _ =>
        val opMetrics: OperatorMetrics = mergeMetrics(operatorMetrics)
        curChild.asInstanceOf[TransformSupport].updateNativeMetrics(opMetrics)

        var newOperatorIdx: java.lang.Long = operatorIdx - 1
        var newMetricsIdx: Int = curMetricsIdx

        if (curChild.getChild == null) {
          return (newOperatorIdx, newMetricsIdx)
        }
        curChild.getChild match {
          case transformer: TransformSupport =>
            val result = updateTransformerMetrics(transformer, relMap, newOperatorIdx,
              metrics, newMetricsIdx, joinParamsMap, aggParamsMap)
            newOperatorIdx = result._1
            newMetricsIdx = result._2
          case _ =>
        }
        (newOperatorIdx, newMetricsIdx)
    }
  }

  var nativeMetricsUpdated: Boolean = false

  /**
   * Update metrics fetched from certain iterator to transformers.
   * @param resIter the iterator to fetch metrics from
   * @param relMap the map between operator index and its rels
   * @param joinParamsMap the map between operator index and join parameters
   * @param aggParamsMap the map between operator index and aggregation parameters
   */
  def updateNativeMetrics(resIter: GeneralOutIterator,
                          relMap: java.util.HashMap[java.lang.Long,
                            java.util.ArrayList[java.lang.Long]],
                          joinParamsMap: java.util.HashMap[java.lang.Long, JoinParams],
                          aggParamsMap: java.util.HashMap[java.lang.Long, AggregationParams])
    : Unit = {
    if (nativeMetricsUpdated) return
    try {
      val metrics = resIter.getMetrics
      val numNativeMetrics = metrics.inputRows.length
      if (numNativeMetrics == 0) return
      if (child == null) return

      updateTransformerMetrics(
        child.asInstanceOf[TransformSupport],
        relMap,
        new java.lang.Long(relMap.size() - 1),
        metrics,
        numNativeMetrics - 1,
        joinParamsMap,
        aggParamsMap)
    } catch {
      case e: Throwable =>
        logWarning(s"Updating native metrics failed due to ${e.getCause}.")
    }
    nativeMetricsUpdated = true
  }

  /**
   * Return built cpp library's signature
   */
  def doBuild(): String = {
    ""
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = child match {
    case c: TransformSupport =>
      c.columnarInputRDDs
    case _ =>
      throw new UnsupportedOperationException
  }

  // Recreate the broadcast build side rdd with matched partition number.
  // Used when whole stage transformer contains scan.
  def genFirstNewRDDsForBroadcast(rddSeq: Seq[RDD[ColumnarBatch]], partitions: Int)
  : Seq[RDD[ColumnarBatch]] = {
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
