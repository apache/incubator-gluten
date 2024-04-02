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

import org.apache.gluten.{GlutenConfig, GlutenNumaBindingInfo}
import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.expression._
import org.apache.gluten.extension.GlutenPlan
import org.apache.gluten.metrics.{GlutenTimeMetric, MetricsUpdater, NoopMetricsUpdater}
import org.apache.gluten.substrait.`type`.{TypeBuilder, TypeNode}
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.plan.{PlanBuilder, PlanNode}
import org.apache.gluten.substrait.rel.{RelNode, SplitInfo}
import org.apache.gluten.utils.SubstraitPlanPrinterUtil

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.softaffinity.SoftAffinity
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.collect.Lists

import scala.collection.mutable

case class TransformContext(
    inputAttributes: Seq[Attribute],
    outputAttributes: Seq[Attribute],
    root: RelNode)

case class WholeStageTransformContext(root: PlanNode, substraitContext: SubstraitContext = null)

trait TransformSupport extends GlutenPlan {

  final override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} doesn't support doExecute")
  }

  final override lazy val supportsColumnar: Boolean = true

  /**
   * Returns all the RDDs of ColumnarBatch which generates the input rows.
   *
   * @note
   *   Right now we support up to two RDDs
   */
  def columnarInputRDDs: Seq[RDD[ColumnarBatch]]

  def doTransform(context: SubstraitContext): TransformContext = {
    throw new UnsupportedOperationException(
      s"This operator doesn't support doTransform with SubstraitContext.")
  }

  def metricsUpdater(): MetricsUpdater

  protected def getColumnarInputRDDs(plan: SparkPlan): Seq[RDD[ColumnarBatch]] = {
    plan match {
      case c: TransformSupport =>
        c.columnarInputRDDs
      case _ =>
        Seq(plan.executeColumnar())
    }
  }
}

trait LeafTransformSupport extends TransformSupport with LeafExecNode {
  final override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = Seq.empty
}

trait UnaryTransformSupport extends TransformSupport with UnaryExecNode {
  final override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    getColumnarInputRDDs(child)
  }
}

case class WholeStageTransformer(child: SparkPlan, materializeInput: Boolean = false)(
    val transformStageId: Int
) extends GenerateTreeStringShim
  with UnaryTransformSupport {
  assert(child.isInstanceOf[TransformSupport])

  def stageId: Int = transformStageId

  def wholeStageTransformerContextDefined: Boolean = wholeStageTransformerContext.isDefined

  // For WholeStageCodegen-like operator, only pipeline time will be handled in graph plotting.
  // See SparkPlanGraph.scala:205 for reference.
  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genWholeStageTransformerMetrics(sparkContext)

  val sparkConf: SparkConf = sparkContext.getConf
  val numaBindingInfo: GlutenNumaBindingInfo = GlutenConfig.getConf.numaBindingInfo
  val substraitPlanLogLevel: String = GlutenConfig.getConf.substraitPlanLogLevel

  @transient
  private var wholeStageTransformerContext: Option[WholeStageTransformContext] = None

  private var outputSchemaForPlan: Option[TypeNode] = None

  private def inferSchemaFromAttributes(attrs: Seq[Attribute]): TypeNode = {
    val outputTypeNodeList = new java.util.ArrayList[TypeNode]()
    for (attr <- attrs) {
      outputTypeNodeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
    }

    TypeBuilder.makeStruct(false, outputTypeNodeList)
  }

  def setOutputSchemaForPlan(expectOutput: Seq[Attribute]): Unit = {
    if (outputSchemaForPlan.isDefined) {
      return
    }

    // Fixes issue-1874
    outputSchemaForPlan = Some(inferSchemaFromAttributes(expectOutput))
  }

  def substraitPlan: PlanNode = {
    if (wholeStageTransformerContext.isDefined) {
      wholeStageTransformerContext.get.root
    } else {
      generateWholeStageTransformContext().root
    }
  }

  def substraitPlanJson: String = {
    SubstraitPlanPrinterUtil.substraitPlanToJson(substraitPlan.toProtobuf)
  }

  def nativePlanString(details: Boolean = true): String = {
    BackendsApiManager.getTransformerApiInstance.getNativePlanString(
      substraitPlan.toProtobuf.toByteArray,
      details)
  }

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def otherCopyArgs: Seq[AnyRef] = Seq(transformStageId.asInstanceOf[Integer])

  // It's misleading with "Codegen" used. But we have to keep "WholeStageCodegen" prefixed to
  // make whole stage transformer clearly plotted in UI, like spark's whole stage codegen.
  // See buildSparkPlanGraphNode in SparkPlanGraph.scala of Spark.
  override def nodeName: String = s"WholeStageCodegenTransformer ($transformStageId)"

  override def verboseStringWithOperatorId(): String = {
    val nativePlan = if (conf.getConf(GlutenConfig.INJECT_NATIVE_PLAN_STRING_TO_EXPLAIN)) {
      s"Native Plan:\n${nativePlanString()}"
    } else {
      ""
    }
    super.verboseStringWithOperatorId() ++ nativePlan
  }

  private def generateWholeStageTransformContext(): WholeStageTransformContext = {
    val substraitContext = new SubstraitContext
    val childCtx = child
      .asInstanceOf[TransformSupport]
      .doTransform(substraitContext)
    if (childCtx == null) {
      throw new NullPointerException(s"WholeStageTransformer can't do Transform on $child")
    }

    val outNames = new java.util.ArrayList[String]()
    for (attr <- childCtx.outputAttributes) {
      outNames.add(ConverterUtils.genColumnNameWithExprId(attr))
    }

    val planNode = if (BackendsApiManager.getSettings.needOutputSchemaForPlan()) {
      val outputSchema = if (outputSchemaForPlan.isDefined) {
        outputSchemaForPlan.get
      } else {
        inferSchemaFromAttributes(childCtx.outputAttributes)
      }

      PlanBuilder.makePlan(
        substraitContext,
        Lists.newArrayList(childCtx.root),
        outNames,
        outputSchema,
        null)
    } else {
      for (attr <- childCtx.outputAttributes) {
        outNames.add(ConverterUtils.genColumnNameWithExprId(attr))
      }
      PlanBuilder.makePlan(substraitContext, Lists.newArrayList(childCtx.root), outNames)
    }

    WholeStageTransformContext(planNode, substraitContext)
  }

  def doWholeStageTransform(): WholeStageTransformContext = {
    // invoke SparkPlan.prepare to do subquery preparation etc.
    super.prepare()
    val context = generateWholeStageTransformContext()
    if (conf.getConf(GlutenConfig.CACHE_WHOLE_STAGE_TRANSFORMER_CONTEXT)) {
      wholeStageTransformerContext = Some(context)
    }
    context
  }

  /** Find all BasicScanExecTransformer in one WholeStageTransformer */
  private def findAllScanTransformers(): Seq[BasicScanExecTransformer] = {
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
        // SHJ may include two scans in a whole stage.
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
    basicScanExecTransformers
  }

  override def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val pipelineTime: SQLMetric = longMetric("pipelineTime")
    val inputRDDs = new ColumnarInputRDDsWrapper(columnarInputRDDs)
    // Check if BatchScan exists.
    val basicScanExecTransformers = findAllScanTransformers()

    if (basicScanExecTransformers.nonEmpty) {

      /**
       * If containing scan exec transformer this "whole stage" generates a RDD which itself takes
       * care of SCAN there won't be any other RDD for SCAN. As a result, genFirstStageIterator
       * rather than genFinalStageIterator will be invoked
       */
      val allScanPartitions = basicScanExecTransformers.map(_.getPartitions)
      val allScanSplitInfos =
        getSplitInfosFromPartitions(basicScanExecTransformers, allScanPartitions)

      val (wsCtx, inputPartitions) = GlutenTimeMetric.withMillisTime {
        val wsCtx = doWholeStageTransform()
        val partitions =
          BackendsApiManager.getIteratorApiInstance.genPartitions(
            wsCtx,
            allScanSplitInfos,
            basicScanExecTransformers)

        (wsCtx, partitions)
      }(
        t =>
          logOnLevel(
            substraitPlanLogLevel,
            s"$nodeName generating the substrait plan took: $t ms."))

      val rdd = new GlutenWholeStageColumnarRDD(
        sparkContext,
        inputPartitions,
        inputRDDs,
        pipelineTime,
        leafMetricsUpdater().updateInputMetrics,
        BackendsApiManager.getMetricsApiInstance.metricsUpdatingFunction(
          child,
          wsCtx.substraitContext.registeredRelMap,
          wsCtx.substraitContext.registeredJoinParams,
          wsCtx.substraitContext.registeredAggregationParams
        )
      )
      (0 until allScanPartitions.head.size).foreach(
        i => {
          val currentPartitions = allScanPartitions.map(_(i))
          currentPartitions.indices.foreach(
            i =>
              currentPartitions(i) match {
                case f: FilePartition =>
                  SoftAffinity.updateFilePartitionLocations(f, rdd.id)
                case _ =>
              })
        })
      rdd
    } else {

      /**
       * the whole stage contains NO BasicScanExecTransformer. this the default case for:
       *   1. SCAN with clickhouse backend (check ColumnarCollapseTransformStages#separateScanRDD())
       *      2. test case where query plan is constructed from simple dataframes (e.g.
       *      GlutenDataFrameAggregateSuite) in these cases, separate RDDs takes care of SCAN as a
       *      result, genFinalStageIterator rather than genFirstStageIterator will be invoked
       */
      val resCtx = GlutenTimeMetric.withMillisTime(doWholeStageTransform()) {
        t =>
          logOnLevel(substraitPlanLogLevel, s"$nodeName generating the substrait plan took: $t ms.")
      }
      new WholeStageZippedPartitionsRDD(
        sparkContext,
        inputRDDs,
        numaBindingInfo,
        sparkConf,
        resCtx,
        pipelineTime,
        BackendsApiManager.getMetricsApiInstance.metricsUpdatingFunction(
          child,
          resCtx.substraitContext.registeredRelMap,
          resCtx.substraitContext.registeredJoinParams,
          resCtx.substraitContext.registeredAggregationParams
        ),
        materializeInput
      )
    }
  }

  override def metricsUpdater(): MetricsUpdater = {
    child match {
      case transformer: TransformSupport => transformer.metricsUpdater()
      case _ => NoopMetricsUpdater
    }
  }

  private def leafMetricsUpdater(): MetricsUpdater = {
    child
      .find {
        case t: TransformSupport if t.children.forall(!_.isInstanceOf[TransformSupport]) => true
        case _ => false
      }
      .map(_.asInstanceOf[TransformSupport].metricsUpdater())
      .getOrElse(NoopMetricsUpdater)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WholeStageTransformer =
    copy(child = newChild, materializeInput = materializeInput)(transformStageId)

  private def getSplitInfosFromPartitions(
      basicScanExecTransformers: Seq[BasicScanExecTransformer],
      allScanPartitions: Seq[Seq[InputPartition]]): Seq[Seq[SplitInfo]] = {
    // If these are two scan transformers, they must have same partitions,
    // otherwise, exchange will be inserted. We should combine the two scan
    // transformers' partitions with same index, and set them together in
    // the substraitContext. We use transpose to do that, You can refer to
    // the diagram below.
    // scan1  p11 p12 p13 p14 ... p1n
    // scan2  p21 p22 p23 p24 ... p2n
    // transpose =>
    // scan1 | scan2
    //  p11  |  p21    => substraitContext.setSplitInfo([p11, p21])
    //  p12  |  p22    => substraitContext.setSplitInfo([p11, p22])
    //  p13  |  p23    ...
    //  p14  |  p24
    //      ...
    //  p1n  |  p2n    => substraitContext.setSplitInfo([p1n, p2n])
    val allScanSplitInfos =
      allScanPartitions.zip(basicScanExecTransformers).map {
        case (partition, transformer) => transformer.getSplitInfosFromPartitions(partition)
      }
    val partitionLength = allScanSplitInfos.head.size
    if (allScanSplitInfos.exists(_.size != partitionLength)) {
      throw new GlutenException(
        "The partition length of all the scan transformer are not the same.")
    }
    allScanSplitInfos.transpose
  }
}

/**
 * This `columnarInputRDDs` would contain [[BroadcastBuildSideRDD]], but the dependency and
 * partition of [[BroadcastBuildSideRDD]] is meaningless. [[BroadcastBuildSideRDD]] should only be
 * used to hold the broadcast value and generate iterator for join.
 */
class ColumnarInputRDDsWrapper(columnarInputRDDs: Seq[RDD[ColumnarBatch]]) extends Serializable {
  def getDependencies: Seq[Dependency[ColumnarBatch]] = {
    assert(
      columnarInputRDDs
        .filterNot(_.isInstanceOf[BroadcastBuildSideRDD])
        .map(_.partitions.length)
        .toSet
        .size <= 1)

    columnarInputRDDs.flatMap {
      case _: BroadcastBuildSideRDD => Nil
      case rdd => new OneToOneDependency[ColumnarBatch](rdd) :: Nil
    }
  }

  def getPartitions(index: Int): Seq[Partition] = {
    columnarInputRDDs.filterNot(_.isInstanceOf[BroadcastBuildSideRDD]).map(_.partitions(index))
  }

  def getPartitionLength: Int = {
    assert(columnarInputRDDs.nonEmpty)
    val nonBroadcastRDD = columnarInputRDDs.find(!_.isInstanceOf[BroadcastBuildSideRDD])
    assert(nonBroadcastRDD.isDefined)
    nonBroadcastRDD.get.partitions.length
  }

  def getIterators(
      inputColumnarRDDPartitions: Seq[Partition],
      context: TaskContext): Seq[Iterator[ColumnarBatch]] = {
    var index = 0
    columnarInputRDDs.flatMap {
      case broadcast: BroadcastBuildSideRDD =>
        broadcast.genBroadcastBuildSideIterator() :: Nil
      case cartesian: CartesianColumnarBatchRDD =>
        val partition =
          inputColumnarRDDPartitions(index).asInstanceOf[CartesianColumnarBatchRDDPartition]
        index += 1
        cartesian.getIterators(partition, context)
      case rdd =>
        val it = rdd.iterator(inputColumnarRDDPartitions(index), context)
        index += 1
        it :: Nil
    }
  }
}
