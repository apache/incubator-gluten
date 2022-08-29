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

import java.{lang, util}
import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}
import com.google.common.collect.Lists
import com.google.protobuf.{Any, ByteString}
import io.glutenproject.GlutenConfig
import io.glutenproject.execution.HashJoinLikeExecTransformer.{makeAndExpression, makeEqualToExpression}
import io.glutenproject.expression._
import io.glutenproject.substrait.{JoinParams, SubstraitContext}
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.vectorized.Metrics.SingleMetric
import io.glutenproject.vectorized.{ExpressionEvaluator, OperatorMetrics}
import io.substrait.proto.JoinRel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{BaseJoinExec, BuildSideRelation, HashJoin, ShuffledJoin}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{BooleanType, DataType}
import org.apache.spark.sql.vectorized.ColumnarBatch

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
      HashClusteredDistribution(leftKeys) :: HashClusteredDistribution(rightKeys) :: Nil
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

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
abstract class HashJoinLikeExecTransformer(leftKeys: Seq[Expression],
                                           rightKeys: Seq[Expression],
                                           joinType: JoinType,
                                           buildSide: BuildSide,
                                           condition: Option[Expression],
                                           left: SparkPlan,
                                           right: SparkPlan)
  extends BaseJoinExec
    with TransformSupport
    with ColumnarShuffledJoin {

  override lazy val metrics = Map(
    "streamInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of stream side input rows"),
    "streamInputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of stream side input vectors"),
    "streamInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of stream side input bytes"),
    "streamRawInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of stream side raw input rows"),
    "streamRawInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of stream side raw input bytes"),
    "streamOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of stream side output rows"),
    "streamOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of stream side output vectors"),
    "streamOutputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of stream side output bytes"),
    "streamCount" -> SQLMetrics.createMetric(
      sparkContext, "stream side cpu wall time count"),
    "streamWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime_stream_input"),
    "streamVeloxToArrow" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime_velox_to_arrow_converter"),
    "streamPeakMemoryBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "stream side peak memory bytes"),
    "streamNumMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of stream side memory allocations"),

    "streamPreProjectionInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of stream preProjection input rows"),
    "streamPreProjectionInputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of stream preProjection input vectors"),
    "streamPreProjectionInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of stream preProjection input bytes"),
    "streamPreProjectionRawInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of stream preProjection raw input rows"),
    "streamPreProjectionRawInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of stream preProjection raw input bytes"),
    "streamPreProjectionOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of stream preProjection output rows"),
    "streamPreProjectionOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of stream preProjection output vectors"),
    "streamPreProjectionOutputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of stream preProjection output bytes"),
    "streamPreProjectionCount" -> SQLMetrics.createMetric(
      sparkContext, "stream preProjection cpu wall time count"),
    "streamPreProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime_stream_preProjection"),
    "streamPreProjectionPeakMemoryBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "stream preProjection peak memory bytes"),
    "streamPreProjectionNumMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of stream preProjection memory allocations"),

    "buildInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of build side input rows"),
    "buildInputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of build side input vectors"),
    "buildInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of build side input bytes"),
    "buildRawInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of build side raw input rows"),
    "buildRawInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of build side raw input bytes"),
    "buildOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of build side output rows"),
    "buildOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of build side output vectors"),
    "buildOutputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of build side output bytes"),
    "buildCount" -> SQLMetrics.createMetric(
      sparkContext, "build side cpu wall time count"),
    "buildWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime_build_input"),
    "buildPeakMemoryBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "build side peak memory bytes"),
    "buildNumMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of build side memory allocations"),

    "buildPreProjectionInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of build preProjection input rows"),
    "buildPreProjectionInputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of build preProjection input vectors"),
    "buildPreProjectionInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of build preProjection input bytes"),
    "buildPreProjectionRawInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of build preProjection raw input rows"),
    "buildPreProjectionRawInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of build preProjection raw input bytes"),
    "buildPreProjectionOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of build preProjection output rows"),
    "buildPreProjectionOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of build preProjection output vectors"),
    "buildPreProjectionOutputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of build preProjection output bytes"),
    "buildPreProjectionCount" -> SQLMetrics.createMetric(
      sparkContext, "build preProjection cpu wall time count"),
    "buildPreProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime_build_preProjection"),
    "buildPreProjectionPeakMemoryBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "build preProjection peak memory bytes"),
    "buildPreProjectionNumMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of build preProjection memory allocations"),

    "hashBuildInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of hash build input rows"),
    "hashBuildInputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of hash build input vectors"),
    "hashBuildInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of hash build input bytes"),
    "hashBuildRawInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of hash build raw input rows"),
    "hashBuildRawInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of hash build raw input bytes"),
    "hashBuildOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of hash build output rows"),
    "hashBuildOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of hash build output vectors"),
    "hashBuildOutputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of hash build output bytes"),
    "hashBuildCount" -> SQLMetrics.createMetric(
      sparkContext, "hash build cpu wall time count"),
    "hashBuildWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime_hashbuild"),
    "hashBuildPeakMemoryBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "hash build peak memory bytes"),
    "hashBuildNumMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of hash build memory allocations"),

    "hashProbeInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of hash probe input rows"),
    "hashProbeInputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of hash probe input vectors"),
    "hashProbeInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of hash probe input bytes"),
    "hashProbeRawInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of hash probe raw input rows"),
    "hashProbeRawInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of hash probe raw input bytes"),
    "hashProbeOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of hash probe output rows"),
    "hashProbeOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of hash probe output vectors"),
    "hashProbeOutputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of hash probe output bytes"),
    "hashProbeCount" -> SQLMetrics.createMetric(
      sparkContext, "hash probe cpu wall time count"),
    "hashProbeWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime_hashprobe"),
    "hashProbePeakMemoryBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "hash probe peak memory bytes"),
    "hashProbeNumMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of hash probe memory allocations"),
    "hashProbeReplacedWithDynamicFilterRows" -> SQLMetrics.createMetric(
      sparkContext, "number of hash probe replaced with dynamic filter rows"),
    "hashProbeDynamicFiltersProduced" -> SQLMetrics.createMetric(
      sparkContext, "number of hash probe dynamic filters produced"),

    "postProjectionInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of postProjection input rows"),
    "postProjectionInputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of postProjection input vectors"),
    "postProjectionInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of postProjection input bytes"),
    "postProjectionRawInputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of postProjection raw input rows"),
    "postProjectionRawInputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of postProjection raw input bytes"),
    "postProjectionOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of postProjection output rows"),
    "postProjectionOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of postProjection output vectors"),
    "postProjectionOutputBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "number of postProjection output bytes"),
    "postProjectionCount" -> SQLMetrics.createMetric(
      sparkContext, "postProjection cpu wall time count"),
    "postProjectionWallNanos" -> SQLMetrics.createNanoTimingMetric(
      sparkContext, "totaltime_postProjection"),
    "postProjectionPeakMemoryBytes" -> SQLMetrics.createSizeMetric(
      sparkContext, "postProjection peak memory bytes"),
    "postProjectionNumMemoryAllocations" -> SQLMetrics.createMetric(
      sparkContext, "number of postProjection memory allocations"),

    "finalOutputRows" -> SQLMetrics.createMetric(
      sparkContext, "number of final output rows"),
    "finalOutputVectors" -> SQLMetrics.createMetric(
      sparkContext, "number of final output vectors"))

  val streamInputRows: SQLMetric = longMetric("streamInputRows")
  val streamInputVectors: SQLMetric = longMetric("streamInputVectors")
  val streamInputBytes: SQLMetric = longMetric("streamInputBytes")
  val streamRawInputRows: SQLMetric = longMetric("streamRawInputRows")
  val streamRawInputBytes: SQLMetric = longMetric("streamRawInputBytes")
  val streamOutputRows: SQLMetric = longMetric("streamOutputRows")
  val streamOutputVectors: SQLMetric = longMetric("streamOutputVectors")
  val streamOutputBytes: SQLMetric = longMetric("streamOutputBytes")
  val streamCount: SQLMetric = longMetric("streamCount")
  val streamWallNanos: SQLMetric = longMetric("streamWallNanos")
  val streamVeloxToArrow: SQLMetric = longMetric("streamVeloxToArrow")
  val streamPeakMemoryBytes: SQLMetric = longMetric("streamPeakMemoryBytes")
  val streamNumMemoryAllocations: SQLMetric = longMetric("streamNumMemoryAllocations")

  val streamPreProjectionInputRows: SQLMetric = longMetric("streamPreProjectionInputRows")
  val streamPreProjectionInputVectors: SQLMetric = longMetric("streamPreProjectionInputVectors")
  val streamPreProjectionInputBytes: SQLMetric = longMetric("streamPreProjectionInputBytes")
  val streamPreProjectionRawInputRows: SQLMetric = longMetric("streamPreProjectionRawInputRows")
  val streamPreProjectionRawInputBytes: SQLMetric = longMetric("streamPreProjectionRawInputBytes")
  val streamPreProjectionOutputRows: SQLMetric = longMetric("streamPreProjectionOutputRows")
  val streamPreProjectionOutputVectors: SQLMetric = longMetric("streamPreProjectionOutputVectors")
  val streamPreProjectionOutputBytes: SQLMetric = longMetric("streamPreProjectionOutputBytes")
  val streamPreProjectionCount: SQLMetric = longMetric("streamPreProjectionCount")
  val streamPreProjectionWallNanos: SQLMetric = longMetric("streamPreProjectionWallNanos")
  val streamPreProjectionPeakMemoryBytes: SQLMetric =
    longMetric("streamPreProjectionPeakMemoryBytes")
  val streamPreProjectionNumMemoryAllocations: SQLMetric =
    longMetric("streamPreProjectionNumMemoryAllocations")

  val buildInputRows: SQLMetric = longMetric("buildInputRows")
  val buildInputVectors: SQLMetric = longMetric("buildInputVectors")
  val buildInputBytes: SQLMetric = longMetric("buildInputBytes")
  val buildRawInputRows: SQLMetric = longMetric("buildRawInputRows")
  val buildRawInputBytes: SQLMetric = longMetric("buildRawInputBytes")
  val buildOutputRows: SQLMetric = longMetric("buildOutputRows")
  val buildOutputVectors: SQLMetric = longMetric("buildOutputVectors")
  val buildOutputBytes: SQLMetric = longMetric("buildOutputBytes")
  val buildCount: SQLMetric = longMetric("buildCount")
  val buildWallNanos: SQLMetric = longMetric("buildWallNanos")
  val buildPeakMemoryBytes: SQLMetric = longMetric("buildPeakMemoryBytes")
  val buildNumMemoryAllocations: SQLMetric = longMetric("buildNumMemoryAllocations")

  val buildPreProjectionInputRows: SQLMetric = longMetric("buildPreProjectionInputRows")
  val buildPreProjectionInputVectors: SQLMetric = longMetric("buildPreProjectionInputVectors")
  val buildPreProjectionInputBytes: SQLMetric = longMetric("buildPreProjectionInputBytes")
  val buildPreProjectionRawInputRows: SQLMetric = longMetric("buildPreProjectionRawInputRows")
  val buildPreProjectionRawInputBytes: SQLMetric = longMetric("buildPreProjectionRawInputBytes")
  val buildPreProjectionOutputRows: SQLMetric = longMetric("buildPreProjectionOutputRows")
  val buildPreProjectionOutputVectors: SQLMetric = longMetric("buildPreProjectionOutputVectors")
  val buildPreProjectionOutputBytes: SQLMetric = longMetric("buildPreProjectionOutputBytes")
  val buildPreProjectionCount: SQLMetric = longMetric("buildPreProjectionCount")
  val buildPreProjectionWallNanos: SQLMetric = longMetric("buildPreProjectionWallNanos")
  val buildPreProjectionPeakMemoryBytes: SQLMetric =
    longMetric("buildPreProjectionPeakMemoryBytes")
  val buildPreProjectionNumMemoryAllocations: SQLMetric =
    longMetric("buildPreProjectionNumMemoryAllocations")

  val hashBuildInputRows: SQLMetric = longMetric("hashBuildInputRows")
  val hashBuildInputVectors: SQLMetric = longMetric("hashBuildInputVectors")
  val hashBuildInputBytes: SQLMetric = longMetric("hashBuildInputBytes")
  val hashBuildRawInputRows: SQLMetric = longMetric("hashBuildRawInputRows")
  val hashBuildRawInputBytes: SQLMetric = longMetric("hashBuildRawInputBytes")
  val hashBuildOutputRows: SQLMetric = longMetric("hashBuildOutputRows")
  val hashBuildOutputVectors: SQLMetric = longMetric("hashBuildOutputVectors")
  val hashBuildOutputBytes: SQLMetric = longMetric("hashBuildOutputBytes")
  val hashBuildCount: SQLMetric = longMetric("hashBuildCount")
  val hashBuildWallNanos: SQLMetric = longMetric("hashBuildWallNanos")
  val hashBuildPeakMemoryBytes: SQLMetric = longMetric("hashBuildPeakMemoryBytes")
  val hashBuildNumMemoryAllocations: SQLMetric = longMetric("hashBuildNumMemoryAllocations")

  val hashProbeInputRows: SQLMetric = longMetric("hashProbeInputRows")
  val hashProbeInputVectors: SQLMetric = longMetric("hashProbeInputVectors")
  val hashProbeInputBytes: SQLMetric = longMetric("hashProbeInputBytes")
  val hashProbeRawInputRows: SQLMetric = longMetric("hashProbeRawInputRows")
  val hashProbeRawInputBytes: SQLMetric = longMetric("hashProbeRawInputBytes")
  val hashProbeOutputRows: SQLMetric = longMetric("hashProbeOutputRows")
  val hashProbeOutputVectors: SQLMetric = longMetric("hashProbeOutputVectors")
  val hashProbeOutputBytes: SQLMetric = longMetric("hashProbeOutputBytes")
  val hashProbeCount: SQLMetric = longMetric("hashProbeCount")
  val hashProbeWallNanos: SQLMetric = longMetric("hashProbeWallNanos")
  val hashProbePeakMemoryBytes: SQLMetric = longMetric("hashProbePeakMemoryBytes")
  val hashProbeNumMemoryAllocations: SQLMetric = longMetric("hashProbeNumMemoryAllocations")

  // The number of rows which were passed through without any processing
  // after filter was pushed down.
  val hashProbeReplacedWithDynamicFilterRows: SQLMetric =
    longMetric("hashProbeReplacedWithDynamicFilterRows")

  // The number of dynamic filters this join generated for push down.
  val hashProbeDynamicFiltersProduced: SQLMetric =
    longMetric("hashProbeDynamicFiltersProduced")

  val postProjectionInputRows: SQLMetric = longMetric("postProjectionInputRows")
  val postProjectionInputVectors: SQLMetric = longMetric("postProjectionInputVectors")
  val postProjectionInputBytes: SQLMetric = longMetric("postProjectionInputBytes")
  val postProjectionRawInputRows: SQLMetric = longMetric("postProjectionRawInputRows")
  val postProjectionRawInputBytes: SQLMetric = longMetric("postProjectionRawInputBytes")
  val postProjectionOutputRows: SQLMetric = longMetric("postProjectionOutputRows")
  val postProjectionOutputVectors: SQLMetric = longMetric("postProjectionOutputVectors")
  val postProjectionOutputBytes: SQLMetric = longMetric("postProjectionOutputBytes")
  val postProjectionCount: SQLMetric = longMetric("postProjectionCount")
  val postProjectionWallNanos: SQLMetric = longMetric("postProjectionWallNanos")
  val postProjectionPeakMemoryBytes: SQLMetric = longMetric("postProjectionPeakMemoryBytes")
  val postProjectionNumMemoryAllocations: SQLMetric =
    longMetric("postProjectionNumMemoryAllocations")

  val finalOutputRows: SQLMetric = longMetric("finalOutputRows")
  val finalOutputVectors: SQLMetric = longMetric("finalOutputVectors")
  def isSkewJoin: Boolean = false

  lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  val (buildKeyExprs, streamedKeyExprs) = {
    require(
      leftKeys.map(_.dataType) == rightKeys.map(_.dataType),
      "Join keys from two sides should have same types")
    val lkeys = HashJoin.rewriteKeyExpr(leftKeys)
    val rkeys = HashJoin.rewriteKeyExpr(rightKeys)
    buildSide match {
      case BuildLeft => (lkeys, rkeys)
      case BuildRight => (rkeys, lkeys)
    }
  }

  // Direct output order of substrait join operation
  private val substraitJoinType = joinType match {
    case Inner =>
      JoinRel.JoinType.JOIN_TYPE_INNER
    case FullOuter =>
      JoinRel.JoinType.JOIN_TYPE_OUTER
    case LeftOuter | RightOuter =>
      JoinRel.JoinType.JOIN_TYPE_LEFT
    case LeftSemi =>
      JoinRel.JoinType.JOIN_TYPE_SEMI
    case LeftAnti =>
      JoinRel.JoinType.JOIN_TYPE_ANTI
    case _ =>
      // TODO: Support cross join with Cross Rel
      // TODO: Support existence join
      JoinRel.JoinType.UNRECOGNIZED
  }

  override def updateMetrics(outNumBatches: Long, outNumRows: Long): Unit = {
    finalOutputVectors += outNumBatches
    finalOutputRows += outNumRows
  }

  override def updateNativeMetrics(operatorMetrics: OperatorMetrics): Unit = {
    throw new UnsupportedOperationException(s"updateNativeMetrics is not supported for join.")
  }

  def updateJoinMetrics(joinMetrics: java.util.ArrayList[OperatorMetrics],
                        singleMetrics: SingleMetric,
                        joinParams: JoinParams): Unit = {
    var idx = 0
    if (joinParams.postProjectionNeeded) {
      val metrics = joinMetrics.get(idx)
      postProjectionInputRows += metrics.inputRows
      postProjectionInputVectors += metrics.inputVectors
      postProjectionInputBytes += metrics.inputBytes
      postProjectionRawInputRows += metrics.rawInputRows
      postProjectionRawInputBytes += metrics.rawInputBytes
      postProjectionOutputRows += metrics.outputRows
      postProjectionOutputVectors += metrics.outputVectors
      postProjectionOutputBytes += metrics.outputBytes
      postProjectionCount += metrics.count
      postProjectionWallNanos += metrics.wallNanos
      postProjectionPeakMemoryBytes += metrics.peakMemoryBytes
      postProjectionNumMemoryAllocations += metrics.numMemoryAllocations
      idx += 1
    }

    // HashProbe
    val hashProbeMetrics = joinMetrics.get(idx)
    hashProbeInputRows += hashProbeMetrics.inputRows
    hashProbeInputVectors += hashProbeMetrics.inputVectors
    hashProbeInputBytes += hashProbeMetrics.inputBytes
    hashProbeRawInputRows += hashProbeMetrics.rawInputRows
    hashProbeRawInputBytes += hashProbeMetrics.rawInputBytes
    hashProbeOutputRows += hashProbeMetrics.outputRows
    hashProbeOutputVectors += hashProbeMetrics.outputVectors
    hashProbeOutputBytes += hashProbeMetrics.outputBytes
    hashProbeCount += hashProbeMetrics.count
    hashProbeWallNanos += hashProbeMetrics.wallNanos
    hashProbePeakMemoryBytes += hashProbeMetrics.peakMemoryBytes
    hashProbeNumMemoryAllocations += hashProbeMetrics.numMemoryAllocations
    hashProbeReplacedWithDynamicFilterRows += hashProbeMetrics.numReplacedWithDynamicFilterRows
    hashProbeDynamicFiltersProduced += hashProbeMetrics.numDynamicFiltersProduced
    idx += 1

    // HashBuild
    val hashBuildMetrics = joinMetrics.get(idx)
    hashBuildInputRows += hashBuildMetrics.inputRows
    hashBuildInputVectors += hashBuildMetrics.inputVectors
    hashBuildInputBytes += hashBuildMetrics.inputBytes
    hashBuildRawInputRows += hashBuildMetrics.rawInputRows
    hashBuildRawInputBytes += hashBuildMetrics.rawInputBytes
    hashBuildOutputRows += hashBuildMetrics.outputRows
    hashBuildOutputVectors += hashBuildMetrics.outputVectors
    hashBuildOutputBytes += hashBuildMetrics.outputBytes
    hashBuildCount += hashBuildMetrics.count
    hashBuildWallNanos += hashBuildMetrics.wallNanos
    hashBuildPeakMemoryBytes += hashBuildMetrics.peakMemoryBytes
    hashBuildNumMemoryAllocations += hashBuildMetrics.numMemoryAllocations
    idx += 1

    if (joinParams.buildPreProjectionNeeded) {
      val metrics = joinMetrics.get(idx)
      buildPreProjectionInputRows += metrics.inputRows
      buildPreProjectionInputVectors += metrics.inputVectors
      buildPreProjectionInputBytes += metrics.inputBytes
      buildPreProjectionRawInputRows += metrics.rawInputRows
      buildPreProjectionRawInputBytes += metrics.rawInputBytes
      buildPreProjectionOutputRows += metrics.outputRows
      buildPreProjectionOutputVectors += metrics.outputVectors
      buildPreProjectionOutputBytes += metrics.outputBytes
      buildPreProjectionCount += metrics.count
      buildPreProjectionWallNanos += metrics.wallNanos
      buildPreProjectionPeakMemoryBytes += metrics.peakMemoryBytes
      buildPreProjectionNumMemoryAllocations += metrics.numMemoryAllocations
      idx += 1
    }

    if (joinParams.isBuildReadRel) {
      val metrics = joinMetrics.get(idx)
      buildInputRows += metrics.inputRows
      buildInputBytes += metrics.inputBytes
      buildRawInputRows += metrics.rawInputRows
      buildRawInputBytes += metrics.rawInputBytes
      buildOutputRows += metrics.outputRows
      buildOutputVectors += metrics.outputVectors
      buildOutputBytes += metrics.outputBytes
      buildCount += metrics.count
      buildWallNanos += metrics.wallNanos
      buildPeakMemoryBytes += metrics.peakMemoryBytes
      buildNumMemoryAllocations += metrics.numMemoryAllocations
      idx += 1
    }

    if (joinParams.streamPreProjectionNeeded) {
      val metrics = joinMetrics.get(idx)
      streamPreProjectionInputRows += metrics.inputRows
      streamPreProjectionInputVectors += metrics.inputVectors
      streamPreProjectionInputBytes += metrics.inputBytes
      streamPreProjectionRawInputRows += metrics.rawInputRows
      streamPreProjectionRawInputBytes += metrics.rawInputBytes
      streamPreProjectionOutputRows += metrics.outputRows
      streamPreProjectionOutputVectors += metrics.outputVectors
      streamPreProjectionOutputBytes += metrics.outputBytes
      streamPreProjectionCount += metrics.count
      streamPreProjectionWallNanos += metrics.wallNanos
      streamPreProjectionPeakMemoryBytes += metrics.peakMemoryBytes
      streamPreProjectionNumMemoryAllocations += metrics.numMemoryAllocations
      idx += 1
    }

    if (joinParams.isStreamedReadRel) {
      val metrics = joinMetrics.get(idx)
      streamInputRows += metrics.inputRows
      streamInputVectors += metrics.inputVectors
      streamInputBytes += metrics.inputBytes
      streamRawInputRows += metrics.rawInputRows
      streamRawInputBytes += metrics.rawInputBytes
      streamOutputRows += metrics.outputRows
      streamOutputVectors += metrics.outputVectors
      streamOutputBytes += metrics.outputBytes
      streamCount += metrics.count
      streamWallNanos += metrics.wallNanos
      streamVeloxToArrow += singleMetrics.veloxToArrow
      streamPeakMemoryBytes += metrics.peakMemoryBytes
      streamNumMemoryAllocations += metrics.numMemoryAllocations
      idx += 1
    }
  }

  override def outputPartitioning: Partitioning = buildSide match {
    case BuildLeft =>
      joinType match {
        case _: InnerLike | RightOuter => right.outputPartitioning
        case x =>
          throw new IllegalArgumentException(
            s"HashJoin should not take $x as the JoinType with building left side")
      }
    case BuildRight =>
      joinType match {
        case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin =>
          left.outputPartitioning
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
      createJoinRel(null, null, streamedPlan.output,
        buildPlan.output, substraitContext, substraitContext.nextOperatorId, validation = true)
    } catch {
      case e: Throwable =>
        logDebug(s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}")
        return false
    }
    // Then, validate the generated plan in native engine.
    if (GlutenConfig.getConf.enableNativeValidation) {
      val validator = new ExpressionEvaluator()
      val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
      validator.doValidate(planNode.toProtobuf.toByteArray)
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
    if(joinParams.isBuildReadRel) {
      substraitContext.registerRelToOperator(operatorId)
    }

    if (preProjectionNeeded(streamedKeyExprs)) {
      joinParams.streamPreProjectionNeeded = true
    }
    if (preProjectionNeeded(buildKeyExprs)) {
      joinParams.buildPreProjectionNeeded = true
    }

    val joinRel = createJoinRel(
      inputStreamedRelNode,
      inputBuildRelNode,
      inputStreamedOutput,
      inputBuildOutput,
      substraitContext,
      operatorId)

    substraitContext.registerJoinParam(operatorId, joinParams)

    createTransformContext(joinRel, inputStreamedOutput, inputBuildOutput)
  }

  private def preProjectionNeeded(keyExprs: Seq[Expression]): Boolean = {
    !keyExprs.forall(_.isInstanceOf[AttributeReference])
  }

  private def createPreProjectionIfNeeded(keyExprs: Seq[Expression],
                                          inputNode: RelNode,
                                          inputNodeOutput: Seq[Attribute],
                                          joinOutput: Seq[Attribute],
                                          substraitContext: SubstraitContext,
                                          operatorId: java.lang.Long,
                                          validation: Boolean)
  : (Seq[(ExpressionNode, DataType)], RelNode, Seq[Attribute]) = {
    if (!preProjectionNeeded(keyExprs)) {
      // Skip pre-projection if all keys are [AttributeReference]s,
      // which can be directly converted into SelectionNode.
      val keys = keyExprs.map { expr =>
        (ExpressionConverter.replaceWithExpressionTransformer(expr, joinOutput)
          .asInstanceOf[AttributeReferenceTransformer]
          .doTransform(substraitContext.registeredFunction), expr.dataType)
      }
      (keys, inputNode, inputNodeOutput)
    } else {
      // Pre-projection is constructed from original columns followed by join-key expressions.
      val selectOrigins = inputNodeOutput.indices.map(ExpressionBuilder.makeSelection(_))
      val appendedKeys = keyExprs.flatMap {
        case _: AttributeReference => None
        case expr =>
          Some(
            (ExpressionConverter
              .replaceWithExpressionTransformer(expr, inputNodeOutput)
              .asInstanceOf[ExpressionTransformer]
              .doTransform(substraitContext.registeredFunction), expr.dataType))
      }
      val preProjectNode = RelBuilder.makeProjectRel(
        inputNode,
        new java.util.ArrayList[ExpressionNode](
          (selectOrigins ++ appendedKeys.map(_._1)).asJava),
        createExtensionNode(inputNodeOutput, validation),
        substraitContext,
        operatorId)

      // Compute index for join keys in join outputs.
      val offset = joinOutput.size - inputNodeOutput.size + selectOrigins.size
      val appendedKeysAndIndices = appendedKeys.zipWithIndex.iterator
      val keys = keyExprs.map {
        case a: AttributeReference =>
          // The selection index for original AttributeReference is unchanged.
          (ExpressionConverter.replaceWithExpressionTransformer(a, joinOutput)
            .asInstanceOf[AttributeReferenceTransformer]
            .doTransform(substraitContext.registeredFunction), a.dataType)
        case _ =>
          val (key, idx) = appendedKeysAndIndices.next()
          (ExpressionBuilder.makeSelection(idx + offset), key._2)
      }
      (keys, preProjectNode, inputNodeOutput ++
        appendedKeys.zipWithIndex.map { case (key, idx) =>
          // Create output attributes for appended keys.
          // This is used as place holder for finding the right column indexes in post-join filters.
          AttributeReference(s"col_${idx + offset}", key._2)()
        }
      )
    }
  }


  private def createJoinRel(inputStreamedRelNode: RelNode,
                            inputBuildRelNode: RelNode,
                            inputStreamedOutput: Seq[Attribute],
                            inputBuildOutput: Seq[Attribute],
                            substraitContext: SubstraitContext,
                            operatorId: java.lang.Long,
                            validation: Boolean = false): RelNode = {
    // Create pre-projection for build/streamed plan. Append projected keys to each side.
    val (streamedKeys, streamedRelNode, streamedOutput) = createPreProjectionIfNeeded(
      streamedKeyExprs,
      inputStreamedRelNode,
      inputStreamedOutput,
      inputStreamedOutput,
      substraitContext,
      operatorId,
      validation)

    val (buildKeys, buildRelNode, buildOutput) = createPreProjectionIfNeeded(
      buildKeyExprs,
      inputBuildRelNode,
      inputBuildOutput,
      streamedOutput ++ inputBuildOutput,
      substraitContext,
      operatorId,
      validation)

    // Combine join keys to make a single expression.
    val joinExpressionNode = (streamedKeys zip buildKeys).map {
      case ((leftKey, leftType), (rightKey, rightType)) =>
        makeEqualToExpression(
          leftKey, leftType, rightKey, rightType, substraitContext.registeredFunction)
    }.reduce((l, r) => makeAndExpression(l, r, substraitContext.registeredFunction))

    // Create post-join filter, which will be computed in hash join.
    val postJoinFilter = condition.map {
      expr =>
        ExpressionConverter
          .replaceWithExpressionTransformer(expr, streamedOutput ++ buildOutput)
          .asInstanceOf[ExpressionTransformer]
          .doTransform(substraitContext.registeredFunction)
    }

    // Create JoinRel.
    val joinRel = RelBuilder.makeJoinRel(
      streamedRelNode,
      buildRelNode,
      substraitJoinType,
      joinExpressionNode,
      postJoinFilter.orNull,
      createJoinExtensionNode(streamedOutput ++ buildOutput, validation),
      substraitContext,
      operatorId)

    // Result projection will drop the appended keys, and exchange columns order if BuildLeft.
    val resultProjection = buildSide match {
      case BuildLeft =>
        val (leftOutput, rightOutput) =
          getResultProjectionOutput(inputBuildOutput, inputStreamedOutput)
        // Exchange the order of build and streamed.
        leftOutput.indices.map(idx =>
          ExpressionBuilder.makeSelection(idx + streamedOutput.size)) ++
          rightOutput.indices
            .map(ExpressionBuilder.makeSelection(_))
      case BuildRight =>
        val (leftOutput, rightOutput) =
          getResultProjectionOutput(inputStreamedOutput, inputBuildOutput)
        leftOutput.indices.map(ExpressionBuilder.makeSelection(_)) ++
          rightOutput.indices.map(idx => ExpressionBuilder.makeSelection(idx + streamedOutput.size))
    }

    RelBuilder.makeProjectRel(
      joinRel,
      new java.util.ArrayList[ExpressionNode](resultProjection.asJava),
      createExtensionNode(streamedOutput ++ buildOutput, validation),
      substraitContext,
      operatorId)
  }

  private def createTransformContext(rel: RelNode,
                                     inputStreamedOutput: Seq[Attribute],
                                     inputBuildOutput: Seq[Attribute]): TransformContext = {
    val inputAttributes = buildSide match {
      case BuildLeft => inputBuildOutput ++ inputStreamedOutput
      case BuildRight => inputStreamedOutput ++ inputBuildOutput
    }
    TransformContext(inputAttributes, output, rel)
  }

  private def createEnhancementForValidation(output: Seq[Attribute]): com.google.protobuf.Any = {
    val inputTypeNodes = output.map { attr =>
      ConverterUtils.getTypeNode(attr.dataType, attr.nullable)
    }
    Any.pack(TypeBuilder.makeStruct(
      new util.ArrayList[TypeNode](inputTypeNodes.asJava)).toProtobuf)
  }

  private def createExtensionNode(output: Seq[Attribute],
                                  validation: Boolean): AdvancedExtensionNode = {
    // Use field [enhancement] in a extension node for input type validation.
    if (validation) {
      ExtensionBuilder.makeAdvancedExtension(
        createEnhancementForValidation(output))
    } else {
      null
    }
  }

  private def createJoinExtensionNode(output: Seq[Attribute],
                                      validation: Boolean): AdvancedExtensionNode = {
    // Use field [optimization] in a extension node
    // to send some join parameters through Substrait plan.
    val joinParameters = genJoinParametersBuilder()
    val enhancement = if (validation) {
      createEnhancementForValidation(output)
    } else {
      null
    }
    ExtensionBuilder.makeAdvancedExtension(joinParameters.build(), enhancement)
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
    Any.newBuilder
      .setValue(ByteString.copyFromUtf8(joinParametersStr.toString))
      .setTypeUrl("/google.protobuf.StringValue")
  }

  def genJoinParameters(): (Int, Int, String) = {
    (0, 0, "")
  }

  // The output of result projection should be consistent with ShuffledJoin.output
  private def getResultProjectionOutput(leftOutput: Seq[Attribute],
                                        rightOutput: Seq[Attribute])
    : (Seq[Attribute], Seq[Attribute]) = {
    joinType match {
      case _: InnerLike =>
        (leftOutput, rightOutput)
      case LeftOuter =>
        (leftOutput, rightOutput.map(_.withNullability(true)))
      case RightOuter =>
        (leftOutput.map(_.withNullability(true)), rightOutput)
      case FullOuter =>
        (leftOutput.map(_.withNullability(true)), rightOutput.map(_.withNullability(true)))
      case j: ExistenceJoin =>
        (leftOutput :+ j.exists, Nil)
      case LeftExistence(_) =>
        (leftOutput, Nil)
      case x =>
        throw new IllegalArgumentException(
          s"${
            getClass.getSimpleName
          } not take $x as the JoinType")
    }
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
      ConverterUtils.makeFuncName(ConverterUtils.EQUAL, Seq(leftType, rightType)))

    val expressionNodes = Lists.newArrayList(leftNode, rightNode)
    val typeNode = TypeBuilder.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }

  def makeAndExpression(leftNode: ExpressionNode,
                        rightNode: ExpressionNode,
                        functionMap: java.util.HashMap[String, java.lang.Long]): ExpressionNode = {
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.AND, Seq(BooleanType, BooleanType)))

    val expressionNodes = Lists.newArrayList(leftNode, rightNode)
    val typeNode = TypeBuilder.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

case class ShuffledHashJoinExecTransformer(leftKeys: Seq[Expression],
                                           rightKeys: Seq[Expression],
                                           joinType: JoinType,
                                           buildSide: BuildSide,
                                           condition: Option[Expression],
                                           left: SparkPlan,
                                           right: SparkPlan)
  extends HashJoinLikeExecTransformer(
    leftKeys,
    rightKeys,
    joinType,
    buildSide,
    condition,
    left,
    right) {

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    getColumnarInputRDDs(streamedPlan) ++ getColumnarInputRDDs(buildPlan)
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): ShuffledHashJoinExecTransformer =
    copy(left = newLeft, right = newRight)
}

case class BroadCastHashJoinContext(buildSideJoinKeys: Seq[Expression],
                                    joinType: JoinType,
                                    buildSideStructure: Seq[Attribute],
                                    buildHashTableId: String)

case class BroadcastHashJoinExecTransformer(leftKeys: Seq[Expression],
                                            rightKeys: Seq[Expression],
                                            joinType: JoinType,
                                            buildSide: BuildSide,
                                            condition: Option[Expression],
                                            left: SparkPlan,
                                            right: SparkPlan,
                                            isNullAwareAntiJoin: Boolean = false)
  extends HashJoinLikeExecTransformer(
    leftKeys,
    rightKeys,
    joinType,
    buildSide,
    condition,
    left,
    right) {

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
  override protected def withNewChildrenInternal(
    newLeft: SparkPlan, newRight: SparkPlan): BroadcastHashJoinExecTransformer =
  copy(left = newLeft, right = newRight)
}
