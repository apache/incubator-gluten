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
import com.google.protobuf.{Any, StringValue}
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression._
import io.glutenproject.sql.shims.SparkShimLoader
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.substrait.{JoinParams, SubstraitContext}
import io.glutenproject.vectorized.Metrics.SingleMetric
import io.glutenproject.vectorized.OperatorMetrics
import io.substrait.proto.JoinRel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{BaseJoinExec, BuildSideRelation, HashJoin}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, MapType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.{lang, util}
import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}

trait ColumnarShuffledJoin extends BaseJoinExec {
  def isSkewJoin: Boolean

  override def nodeName: String = {
    if (isSkewJoin) super.nodeName + "(skew=true)" else super.nodeName
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    if (isSkewJoin) {
      // We re-arrange the shuffle partitions to deal with skew join, and the new children
      // partitioning doesn't satisfy `HashClusteredDistribution`.
      UnspecifiedDistribution :: UnspecifiedDistribution :: Nil
    } else {
      SparkShimLoader.getSparkShims.getDistribution(leftKeys, rightKeys)
    }
  }

  override def outputPartitioning: Partitioning = joinType match {
    case _: InnerLike =>
      PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case LeftExistence(_) => left.outputPartitioning
    case x =>
      throw new IllegalArgumentException(
        s"ShuffledJoin should not take $x as the JoinType")
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        (left.output ++ right.output).map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"${getClass.getSimpleName} not take $x as the JoinType")
    }
  }
}

trait HashJoinMetricsUpdater extends MetricsUpdater {
  def updateJoinMetrics(joinMetrics: java.util.ArrayList[OperatorMetrics],
                        singleMetrics: SingleMetric,
                        joinParams: JoinParams): Unit
}

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
trait HashJoinLikeExecTransformer
  extends BaseJoinExec with TransformSupport with ColumnarShuffledJoin {

  def joinBuildSide: BuildSide
  def hashJoinType: JoinType

  override lazy val metrics = Map(
    "hashBuildInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of hash build input rows"),
    "hashBuildOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of hash build output rows"),
    "hashBuildOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of hash build output vectors"),
    "hashBuildOutputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of hash build output bytes"),
    "hashBuildCpuNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "hash build cpu time"),
    "hashBuildWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime of hash build"),
    "hashBuildPeakMemoryBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "hash build peak memory bytes"),
    "hashBuildNumMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of hash build memory allocations"),
    "hashBuildSpilledBytes" -> SQLMetrics.createMetric(
      sparkContext, "total bytes written for spilling of hash build"),
    "hashBuildSpilledRows" -> SQLMetrics.createMetric(
      sparkContext, "total rows written for spilling of hash build"),
    "hashBuildSpilledPartitions" -> SQLMetrics.createMetric(
      sparkContext, "total spilled partitions of hash build"),
    "hashBuildSpilledFiles" -> SQLMetrics.createMetric(
      sparkContext, "total spilled files of hash build"),

    "hashProbeInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of hash probe input rows"),
    "hashProbeOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of hash probe output rows"),
    "hashProbeOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of hash probe output vectors"),
    "hashProbeCpuNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "hash probe cpu time"),
    "hashProbeWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime of hash probe"),
    "hashProbePeakMemoryBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "hash probe peak memory bytes"),
    "hashProbeNumMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of hash probe memory allocations"),
    "hashProbeSpilledBytes" -> SQLMetrics.createMetric(
      sparkContext, "total bytes written for spilling of hash probe"),
    "hashProbeSpilledRows" -> SQLMetrics.createMetric(
      sparkContext, "total rows written for spilling of hash probe"),
    "hashProbeSpilledPartitions" -> SQLMetrics.createMetric(
      sparkContext, "total spilled partitions of hash probe"),
    "hashProbeSpilledFiles" -> SQLMetrics.createMetric(
      sparkContext, "total spilled files of hash probe"),
    "hashProbeReplacedWithDynamicFilterRows" -> SQLMetrics.createMetric(
      sparkContext, "number of hash probe replaced with dynamic filter rows"),
    "hashProbeDynamicFiltersProduced" -> SQLMetrics.createMetric(
      sparkContext, "number of hash probe dynamic filters produced"),

    "streamWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime of stream input"),
    "streamVeloxToArrow" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime of velox2arrow converter"),

    "streamPreProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime of stream preProjection"),

    "buildWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime to build input"),

    "buildPreProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime to build preProjection"),

    "postProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime of postProjection"),
    "postProjectionOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of postProjection output rows"),
    "postProjectionOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of postProjection output vectors"),

    "finalOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of final output rows"),
    "finalOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of final output vectors"))

  object MetricsUpdaterImpl extends HashJoinMetricsUpdater {
    val hashBuildInputRows: SQLMetric = longMetric("hashBuildInputRows")
    val hashBuildOutputRows: SQLMetric = longMetric("hashBuildOutputRows")
    val hashBuildOutputVectors: SQLMetric = longMetric("hashBuildOutputVectors")
    val hashBuildOutputBytes: SQLMetric = longMetric("hashBuildOutputBytes")
    val hashBuildCpuNanos: SQLMetric = longMetric("hashBuildCpuNanos")
    val hashBuildWallNanos: SQLMetric = longMetric("hashBuildWallNanos")
    val hashBuildPeakMemoryBytes: SQLMetric = longMetric("hashBuildPeakMemoryBytes")
    val hashBuildNumMemoryAllocations: SQLMetric = longMetric("hashBuildNumMemoryAllocations")
    val hashBuildSpilledBytes: SQLMetric = longMetric("hashBuildSpilledBytes")
    val hashBuildSpilledRows: SQLMetric = longMetric("hashBuildSpilledRows")
    val hashBuildSpilledPartitions: SQLMetric = longMetric("hashBuildSpilledPartitions")
    val hashBuildSpilledFiles: SQLMetric = longMetric("hashBuildSpilledFiles")

    val hashProbeInputRows: SQLMetric = longMetric("hashProbeInputRows")
    val hashProbeOutputRows: SQLMetric = longMetric("hashProbeOutputRows")
    val hashProbeOutputVectors: SQLMetric = longMetric("hashProbeOutputVectors")
    val hashProbeOutputBytes: SQLMetric = longMetric("hashProbeOutputBytes")
    val hashProbeCpuNanos: SQLMetric = longMetric("hashProbeCpuNanos")
    val hashProbeWallNanos: SQLMetric = longMetric("hashProbeWallNanos")
    val hashProbePeakMemoryBytes: SQLMetric = longMetric("hashProbePeakMemoryBytes")
    val hashProbeNumMemoryAllocations: SQLMetric = longMetric("hashProbeNumMemoryAllocations")
    val hashProbeSpilledBytes: SQLMetric = longMetric("hashProbeSpilledBytes")
    val hashProbeSpilledRows: SQLMetric = longMetric("hashProbeSpilledRows")
    val hashProbeSpilledPartitions: SQLMetric = longMetric("hashProbeSpilledPartitions")
    val hashProbeSpilledFiles: SQLMetric = longMetric("hashProbeSpilledFiles")

    // The number of rows which were passed through without any processing
    // after filter was pushed down.
    val hashProbeReplacedWithDynamicFilterRows: SQLMetric =
    longMetric("hashProbeReplacedWithDynamicFilterRows")

    // The number of dynamic filters this join generated for push down.
    val hashProbeDynamicFiltersProduced: SQLMetric =
      longMetric("hashProbeDynamicFiltersProduced")

    val streamWallNanos: SQLMetric = longMetric("streamWallNanos")
    val streamVeloxToArrow: SQLMetric = longMetric("streamVeloxToArrow")
    val streamPreProjectionWallNanos: SQLMetric = longMetric("streamPreProjectionWallNanos")
    val buildWallNanos: SQLMetric = longMetric("buildWallNanos")
    val buildPreProjectionWallNanos: SQLMetric = longMetric("buildPreProjectionWallNanos")
    val postProjectionWallNanos: SQLMetric = longMetric("postProjectionWallNanos")
    val postProjectionOutputRows: SQLMetric = longMetric("postProjectionOutputRows")
    val postProjectionOutputVectors: SQLMetric = longMetric("postProjectionOutputVectors")

    val finalOutputRows: SQLMetric = longMetric("finalOutputRows")
    val finalOutputVectors: SQLMetric = longMetric("finalOutputVectors")

    override def updateOutputMetrics(outNumBatches: Long, outNumRows: Long): Unit = {
      finalOutputVectors += outNumBatches
      finalOutputRows += outNumRows
    }

    override def updateNativeMetrics(operatorMetrics: OperatorMetrics): Unit = {
      throw new UnsupportedOperationException(s"updateNativeMetrics is not supported for join.")
    }

    override def updateJoinMetrics(joinMetrics: java.util.ArrayList[OperatorMetrics],
                          singleMetrics: SingleMetric,
                          joinParams: JoinParams): Unit = {
      var idx = 0
      if (joinParams.postProjectionNeeded) {
        val postProjectMetrics = joinMetrics.get(idx)
        postProjectionWallNanos += postProjectMetrics.wallNanos
        postProjectionOutputRows += postProjectMetrics.outputRows
        postProjectionOutputVectors += postProjectMetrics.outputVectors
        idx += 1
      }

      // HashProbe
      val hashProbeMetrics = joinMetrics.get(idx)
      hashProbeInputRows += hashProbeMetrics.inputRows
      hashProbeOutputRows += hashProbeMetrics.outputRows
      hashProbeOutputVectors += hashProbeMetrics.outputVectors
      hashProbeOutputBytes += hashProbeMetrics.outputBytes
      hashProbeCpuNanos += hashProbeMetrics.cpuNanos
      hashProbeWallNanos += hashProbeMetrics.wallNanos
      hashProbePeakMemoryBytes += hashProbeMetrics.peakMemoryBytes
      hashProbeNumMemoryAllocations += hashProbeMetrics.numMemoryAllocations
      hashProbeSpilledBytes += hashProbeMetrics.spilledBytes
      hashProbeSpilledRows += hashProbeMetrics.spilledRows
      hashProbeSpilledPartitions += hashProbeMetrics.spilledPartitions
      hashProbeSpilledFiles += hashProbeMetrics.spilledFiles
      hashProbeReplacedWithDynamicFilterRows += hashProbeMetrics.numReplacedWithDynamicFilterRows
      hashProbeDynamicFiltersProduced += hashProbeMetrics.numDynamicFiltersProduced
      idx += 1

      // HashBuild
      val hashBuildMetrics = joinMetrics.get(idx)
      hashBuildInputRows += hashBuildMetrics.inputRows
      hashBuildOutputRows += hashBuildMetrics.outputRows
      hashBuildOutputVectors += hashBuildMetrics.outputVectors
      hashBuildOutputBytes += hashBuildMetrics.outputBytes
      hashBuildCpuNanos += hashBuildMetrics.cpuNanos
      hashBuildWallNanos += hashBuildMetrics.wallNanos
      hashBuildPeakMemoryBytes += hashBuildMetrics.peakMemoryBytes
      hashBuildNumMemoryAllocations += hashBuildMetrics.numMemoryAllocations
      hashBuildSpilledBytes += hashProbeMetrics.spilledBytes
      hashBuildSpilledRows += hashProbeMetrics.spilledRows
      hashBuildSpilledPartitions += hashProbeMetrics.spilledPartitions
      hashBuildSpilledFiles += hashProbeMetrics.spilledFiles
      idx += 1

      if (joinParams.buildPreProjectionNeeded) {
        buildPreProjectionWallNanos += joinMetrics.get(idx).wallNanos
        idx += 1
      }

      if (joinParams.isBuildReadRel) {
        buildWallNanos += joinMetrics.get(idx).wallNanos
        idx += 1
      }

      if (joinParams.streamPreProjectionNeeded) {
        streamPreProjectionWallNanos += joinMetrics.get(idx).wallNanos
        idx += 1
      }

      if (joinParams.isStreamedReadRel) {
        val streamMetrics = joinMetrics.get(idx)
        streamWallNanos += streamMetrics.wallNanos
        streamVeloxToArrow += singleMetrics.veloxToArrow
        idx += 1
      }
    }
  }

  // Whether the left and right side should be exchanged.
  protected lazy val exchangeTable: Boolean = joinBuildSide match {
      case BuildLeft => true
      case BuildRight => false
  }

  lazy val (buildPlan, streamedPlan) = if (exchangeTable) {
    (left, right)
  } else {
    (right, left)
  }

  def sameType(from: DataType, to: DataType): Boolean = {
    (from, to) match {
      case (ArrayType(fromElement, _), ArrayType(toElement, _)) =>
        sameType(fromElement, toElement)

      case (MapType(fromKey, fromValue, _), MapType(toKey, toValue, _)) =>
        sameType(fromKey, toKey) &&
          sameType(fromValue, toValue)

      case (StructType(fromFields), StructType(toFields)) =>
        fromFields.length == toFields.length &&
          fromFields.zip(toFields).forall { case (l, r) =>
            l.name.equalsIgnoreCase(r.name) &&
              sameType(l.dataType, r.dataType)
          }

      case (fromDataType, toDataType) => fromDataType == toDataType
    }
  }

  val (buildKeyExprs, streamedKeyExprs) = {
    require(leftKeys.length == rightKeys.length &&
      leftKeys.map(_.dataType)
        .zip(rightKeys.map(_.dataType))
        .forall(types => sameType(types._1, types._2)),
      "Join keys from two sides should have same length and types")
    val lkeys = HashJoin.rewriteKeyExpr(leftKeys)
    val rkeys = HashJoin.rewriteKeyExpr(rightKeys)
    if (exchangeTable) {
      (lkeys, rkeys)
    } else {
      (rkeys, lkeys)
    }
  }

  protected val substraitJoinType: JoinRel.JoinType = joinType match {
    case Inner =>
      JoinRel.JoinType.JOIN_TYPE_INNER
    case FullOuter =>
      JoinRel.JoinType.JOIN_TYPE_OUTER
    case LeftOuter | RightOuter =>
      // The right side is required to be used for building hash table in Substrait plan.
      // Therefore, for RightOuter Join, the left and right relations are exchanged and the
      // join type is reverted.
      JoinRel.JoinType.JOIN_TYPE_LEFT
    case LeftSemi =>
      JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
    case LeftAnti =>
      JoinRel.JoinType.JOIN_TYPE_ANTI
    case _ =>
      // TODO: Support cross join with Cross Rel
      // TODO: Support existence join
      JoinRel.JoinType.UNRECOGNIZED
  }

  override def metricsUpdater(): HashJoinMetricsUpdater = MetricsUpdaterImpl

  override def outputPartitioning: Partitioning = joinBuildSide match {
    case BuildLeft =>
      joinType match {
        case _: InnerLike | RightOuter => right.outputPartitioning
        case LeftOuter => left.outputPartitioning
        case x =>
          throw new IllegalArgumentException(
            s"HashJoin should not take $x as the JoinType with building left side")
      }
    case BuildRight =>
      joinType match {
        case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin =>
          left.outputPartitioning
        case RightOuter => right.outputPartitioning
        case x =>
          throw new IllegalArgumentException(
            s"HashJoin should not take $x as the JoinType with building right side")
      }
  }

  override def supportsColumnar: Boolean = true

  override def getBuildPlans: Seq[(SparkPlan, SparkPlan)] = buildPlan match {
    case c: TransformSupport =>
      val childPlans = c.getBuildPlans
      childPlans :+ (this, null)
    case _ =>
      Seq((this, null))
  }

  override def getStreamedLeafPlan: SparkPlan = streamedPlan match {
    case c: TransformSupport =>
      c.getStreamedLeafPlan
    case _ =>
      this
  }

  override def getChild: SparkPlan = streamedPlan

  override def doValidate(): Boolean = {
    val substraitContext = new SubstraitContext
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    if (substraitJoinType == JoinRel.JoinType.UNRECOGNIZED) {
      return false
    }
    val relNode = try {
      JoinUtils.createJoinRel(
        streamedKeyExprs,
        buildKeyExprs,
        condition,
        substraitJoinType,
        exchangeTable,
        joinType,
        genJoinParametersBuilder(),
        null, null, streamedPlan.output,
        buildPlan.output, substraitContext, substraitContext.nextOperatorId, validation = true)
    } catch {
      case e: Throwable =>
        logDebug(s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}")
        return false
    }
    // Then, validate the generated plan in native engine.
    if (GlutenConfig.getConf.enableNativeValidation) {
      val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
      BackendsApiManager.getValidatorApiInstance.doValidate(planNode)
    } else {
      true
    }
  }

  override def doTransform(substraitContext: SubstraitContext): TransformContext = {

    def transformAndGetOutput(plan: SparkPlan): (RelNode, Seq[Attribute], Boolean) = {
      plan match {
        case p: TransformSupport =>
          val transformContext = p.doTransform(substraitContext)
          (transformContext.root, transformContext.outputAttributes, false)
        case _ =>
          val readRel = RelBuilder.makeReadRel(
            new util.ArrayList[Attribute](plan.output.asJava),
            substraitContext,
            new lang.Long(-1)) /* A special handling in Join to delay the rel registration. */
          (readRel, plan.output, true)
      }
    }

    val joinParams = new JoinParams
    val (inputStreamedRelNode, inputStreamedOutput, isStreamedReadRel) =
      transformAndGetOutput(streamedPlan)
    joinParams.isStreamedReadRel = isStreamedReadRel

    val (inputBuildRelNode, inputBuildOutput, isBuildReadRel) =
      transformAndGetOutput(buildPlan)
    joinParams.isBuildReadRel = isBuildReadRel

    // Get the operator id of this Join.
    val operatorId = substraitContext.nextOperatorId

    // Register the ReadRel to correct operator Id.
    if (joinParams.isStreamedReadRel) {
      substraitContext.registerRelToOperator(operatorId)
    }
    if (joinParams.isBuildReadRel) {
      substraitContext.registerRelToOperator(operatorId)
    }

    if (JoinUtils.preProjectionNeeded(streamedKeyExprs)) {
      joinParams.streamPreProjectionNeeded = true
    }
    if (JoinUtils.preProjectionNeeded(buildKeyExprs)) {
      joinParams.buildPreProjectionNeeded = true
    }

    val joinRel = JoinUtils.createJoinRel(
      streamedKeyExprs,
      buildKeyExprs,
      condition,
      substraitJoinType,
      exchangeTable,
      joinType,
      genJoinParametersBuilder(),
      inputStreamedRelNode,
      inputBuildRelNode,
      inputStreamedOutput,
      inputBuildOutput,
      substraitContext,
      operatorId)

    substraitContext.registerJoinParam(operatorId, joinParams)

    JoinUtils.createTransformContext(
      exchangeTable, output, joinRel,
      inputStreamedOutput, inputBuildOutput)
  }

  def genJoinParametersBuilder(): Any.Builder = {
    val (isBHJ, isNullAwareAntiJoin, buildHashTableId) = genJoinParameters()
    // Start with "JoinParameters:"
    val joinParametersStr = new StringBuffer("JoinParameters:")
    // isBHJ: 0 for SHJ, 1 for BHJ
    // isNullAwareAntiJoin: 0 for false, 1 for true
    // buildHashTableId: the unique id for the hash table of build plan
    joinParametersStr.append("isBHJ=").append(isBHJ).append("\n")
      .append("isNullAwareAntiJoin=").append(isNullAwareAntiJoin).append("\n")
      .append("buildHashTableId=").append(buildHashTableId).append("\n")
      .append("isExistenceJoin=").append(
      if (joinType.isInstanceOf[ExistenceJoin]) 1 else 0).append("\n")
    val message = StringValue
      .newBuilder()
      .setValue(joinParametersStr.toString)
      .build()
    Any.newBuilder
      .setValue(message.toByteString)
      .setTypeUrl("/google.protobuf.StringValue")
  }

  def genJoinParameters(): (Int, Int, String) = {
    (0, 0, "")
  }

  override protected def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${
        this.getClass.getSimpleName
      } doesn't support doExecute")
  }
}

object HashJoinLikeExecTransformer {
  def makeEqualToExpression(leftNode: ExpressionNode,
                            leftType: DataType,
                            rightNode: ExpressionNode,
                            rightType: DataType,
                            functionMap: java.util.HashMap[String, java.lang.Long]
                           ): ExpressionNode = {
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ExpressionMappings.EQUAL, Seq(leftType, rightType)))

    val expressionNodes = Lists.newArrayList(leftNode, rightNode)
    val typeNode = TypeBuilder.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }

  def makeAndExpression(leftNode: ExpressionNode,
                        rightNode: ExpressionNode,
                        functionMap: java.util.HashMap[String, java.lang.Long]): ExpressionNode = {
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ExpressionMappings.AND, Seq(BooleanType, BooleanType)))

    val expressionNodes = Lists.newArrayList(leftNode, rightNode)
    val typeNode = TypeBuilder.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

abstract class ShuffledHashJoinExecTransformer(leftKeys: Seq[Expression],
                                               rightKeys: Seq[Expression],
                                               joinType: JoinType,
                                               buildSide: BuildSide,
                                               condition: Option[Expression],
                                               left: SparkPlan,
                                               right: SparkPlan,
                                               isSkewJoin: Boolean)
  extends HashJoinLikeExecTransformer {

  override def joinBuildSide: BuildSide = buildSide
  override def hashJoinType: JoinType = joinType

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    getColumnarInputRDDs(streamedPlan) ++ getColumnarInputRDDs(buildPlan)
  }
}

case class BroadCastHashJoinContext(buildSideJoinKeys: Seq[Expression],
                                    joinType: JoinType,
                                    buildSideStructure: Seq[Attribute],
                                    buildHashTableId: String)

abstract class BroadcastHashJoinExecTransformer(leftKeys: Seq[Expression],
                                                rightKeys: Seq[Expression],
                                                joinType: JoinType,
                                                buildSide: BuildSide,
                                                condition: Option[Expression],
                                                left: SparkPlan,
                                                right: SparkPlan,
                                                isNullAwareAntiJoin: Boolean)
  extends HashJoinLikeExecTransformer {

  override def joinBuildSide: BuildSide = buildSide
  override def hashJoinType: JoinType = joinType
  override def isSkewJoin: Boolean = false

  // Unique ID for builded hash table
  lazy val buildHashTableId = "BuildedHashTable-" + buildPlan.id

  override def genJoinParameters(): (Int, Int, String) = {
    (1, if (isNullAwareAntiJoin) 1 else 0, buildHashTableId)
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    val streamedRDD = getColumnarInputRDDs(streamedPlan)
    val broadcasted = buildSide match {
      case BuildLeft =>
        left.executeBroadcast[BuildSideRelation]()
      case BuildRight =>
        right.executeBroadcast[BuildSideRelation]()
    }

    val context = BroadCastHashJoinContext(
      buildKeyExprs, joinType, buildPlan.output, buildHashTableId)

    val buildRDD = if (streamedRDD.isEmpty) {
      // Stream plan itself contains scan and has no input rdd,
      // so the number of partitions cannot be decided here.
      BroadcastBuildSideRDD(
        sparkContext,
        broadcasted,
        context)
    } else {
      // Try to get the number of partitions from a non-broadcast RDD.
      val nonBroadcastRDD = streamedRDD.find(rdd => !rdd.isInstanceOf[BroadcastBuildSideRDD])
      if (nonBroadcastRDD.isDefined) {
        BroadcastBuildSideRDD(
          sparkContext,
          broadcasted,
          context,
          nonBroadcastRDD.orNull.getNumPartitions)
      } else {
        // When all stream RDDs are broadcast RDD, the number of partitions can be undecided
        // because stream plan may contain scan.
        var partitions = -1
        breakable {
          for (rdd <- streamedRDD) {
            try {
              partitions = rdd.getNumPartitions
              break
            } catch {
              case _: Throwable =>
              // The partitions of this RDD is not decided yet.
            }
          }
        }
        // If all the stream RDDs are broadcast RDD,
        // the number of partitions will be decided later in whole stage transformer.
        BroadcastBuildSideRDD(
          sparkContext,
          broadcasted,
          context,
          partitions)
      }
    }
    streamedRDD :+ buildRDD
  }
}
