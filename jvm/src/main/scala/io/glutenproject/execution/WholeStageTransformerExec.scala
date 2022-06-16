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

import scala.collection.mutable.ListBuffer

import com.google.common.collect.Lists
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression._
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.plan.{PlanBuilder, PlanNode}
import io.glutenproject.substrait.rel.RelNode
import io.glutenproject.vectorized._

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

  val sparkConf = sparkContext.getConf
  val numaBindingInfo = GlutenConfig.getConf.numaBindingInfo
  val enableColumnarSortMergeJoinLazyRead: Boolean =
    GlutenConfig.getConf.enableColumnarSortMergeJoinLazyRead


  // For WholeStageCodegen-like operator, only pipeline time will be handled in graph plotting.
  // See SparkPlanGraph.scala:205 for reference.
  override lazy val metrics = Map(
    "pipelineTime" -> SQLMetrics.createTimingMetric(sparkContext, "duration"))

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

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = {
    child.asInstanceOf[TransformSupport].getBuildPlans
  }

  override def getStreamedLeafPlan: SparkPlan = {
    child.asInstanceOf[TransformSupport].getStreamedLeafPlan
  }

  override def getChild: SparkPlan = child

  /**
   * Update metrics for the child plan.
   * @param outNumBatches the number of batches to add
   * @param outNumRows the number of rows to add
   */
  override def updateMetrics(outNumBatches: Long, outNumRows: Long): Unit = {
    child match {
      // For Filter transformer, need to convert to FilterExecBase
      // to call the overridden updateMetrics.
      case transformer: FilterExecBaseTransformer =>
        transformer.updateMetrics(outNumBatches, outNumRows)
      case _ =>
        child.asInstanceOf[TransformSupport].updateMetrics(outNumBatches, outNumRows)
    }
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
          op.partitions
          Some(currentOp.asInstanceOf[BatchScanExecTransformer])
        case op: FileSourceScanExecTransformer =>
          Some(currentOp.asInstanceOf[FileSourceScanExecTransformer])
      }
    } else {
      None
    }
  }

  override def doExecute(): RDD[InternalRow] = {
    // check if BatchScan exists
    val currentOp = checkBatchScanExecTransformerChild()
    if (currentOp.isDefined) {
      // If containing scan exec transformer, a new RDD is created.
      val fileScan = currentOp.get
      val wsCxt = doWholestageTransform()

      val startTime = System.nanoTime()
      val substraitPlanPartition = fileScan.getPartitions.map(p =>
        BackendsApiManager.getIteratorApiInstance.genNativeFilePartition(p, wsCxt))
      logDebug(
        s"Generated substrait plan tooks: ${(System.nanoTime() - startTime) / 1000000} ms")

      val wsRDD = new NativeWholestageRowRDD(sparkContext, substraitPlanPartition, false)
      wsRDD
    } else {
      sparkContext.emptyRDD
    }
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

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val pipelineTime: SQLMetric = longMetric("pipelineTime")

    val signature = doBuild()
    val listJars = uploadAndListJars(signature)

    // we should zip all dependent RDDs to current main RDD
    // TODO: Does it still need these parameters?
    val streamedSortPlan = getStreamedLeafPlan
    val dependentKernels: ListBuffer[ExpressionEvaluator] = ListBuffer()
    val dependentKernelIterators: ListBuffer[GeneralOutIterator] = ListBuffer()
    val buildRelationBatchHolder: ListBuffer[ColumnarBatch] = ListBuffer()

    val inputRDDs = columnarInputRDDs
    // Check if BatchScan exists.
    val currentOp = checkBatchScanExecTransformerChild()
    if (currentOp.isDefined) {
      // If containing scan exec transformer, a new RDD is created.
      val fileScan = currentOp.get

      val wsCxt = doWholestageTransform()
      // Get and set the file format based on fileScan.
      wsCxt.substraitContext.setFileFormat(ConverterUtils.getFileFormat(fileScan))

      val startTime = System.nanoTime()
      val substraitPlanPartition = fileScan.getPartitions.map(p =>
        BackendsApiManager.getIteratorApiInstance.genNativeFilePartition(p, wsCxt))
      logInfo(
        s"Generating the Substrait plan took: ${(System.nanoTime() - startTime) / 1000000} ms.")

      new NativeWholeStageColumnarRDD(
        sparkContext,
        substraitPlanPartition,
        wsCxt.outputAttributes,
        genFirstNewRDDsForBroadcast(inputRDDs, substraitPlanPartition.size),
        updateMetrics)
    } else {
      val resCtx = doWholestageTransform()
      logDebug(s"Generating substrait plan:\n${resCtx.root.toProtobuf.toString}")

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
            streamedSortPlan,
            pipelineTime,
            updateMetrics,
            buildRelationBatchHolder,
            dependentKernels,
            dependentKernelIterators)
      }

      new WholeStageZippedPartitionsRDD(
        sparkContext, genFinalNewRDDsForBroadcast(inputRDDs), genFinalStageIterator)
    }
  }
}
