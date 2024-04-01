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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.expression._
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.{JoinParams, SubstraitContext}
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.gluten.utils.SubstraitUtil

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{ExpandOutputPartitioningShim, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.joins.{BaseJoinExec, HashedRelationBroadcastMode, HashJoin}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.common.collect.Lists
import com.google.protobuf.{Any, StringValue}
import io.substrait.proto.JoinRel

import java.lang.{Long => JLong}
import java.util.{Map => JMap}

trait ColumnarShuffledJoin extends BaseJoinExec {
  def isSkewJoin: Boolean

  override def nodeName: String = {
    if (isSkewJoin) super.nodeName + "(skew=true)" else super.nodeName
  }

  override def stringArgs: Iterator[scala.Any] = super.stringArgs.toSeq.dropRight(1).iterator

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
      throw new IllegalArgumentException(s"ShuffledJoin should not take $x as the JoinType")
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
        throw new IllegalArgumentException(s"${getClass.getSimpleName} not take $x as the JoinType")
    }
  }
}

/** Performs a hash join of two child relations by first shuffling the data using the join keys. */
trait HashJoinLikeExecTransformer extends BaseJoinExec with TransformSupport {

  def joinBuildSide: BuildSide
  def hashJoinType: JoinType

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics: Map[String, SQLMetric] =
    BackendsApiManager.getMetricsApiInstance.genHashJoinTransformerMetrics(sparkContext)

  // Hint substrait to switch the left and right,
  // since we assume always build right side in substrait.
  protected lazy val needSwitchChildren: Boolean = joinBuildSide match {
    case BuildLeft => true
    case BuildRight => false
  }

  lazy val (buildPlan, streamedPlan) = if (needSwitchChildren) {
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
        fromFields.zip(toFields).forall {
          case (l, r) =>
            l.name.equalsIgnoreCase(r.name) &&
            sameType(l.dataType, r.dataType)
        }

      case (fromDataType, toDataType) => fromDataType == toDataType
    }
  }

  val (buildKeyExprs, streamedKeyExprs) = {
    require(
      leftKeys.length == rightKeys.length &&
        leftKeys
          .map(_.dataType)
          .zip(rightKeys.map(_.dataType))
          .forall(types => sameType(types._1, types._2)),
      "Join keys from two sides should have same length and types"
    )
    // Spark has an improvement which would patch integer joins keys to a Long value.
    // But this improvement would cause add extra project before hash join in velox,
    // disabling this improvement as below would help reduce the project.
    val (lkeys, rkeys) = if (BackendsApiManager.getSettings.enableJoinKeysRewrite()) {
      (HashJoin.rewriteKeyExpr(leftKeys), HashJoin.rewriteKeyExpr(rightKeys))
    } else {
      (leftKeys, rightKeys)
    }
    if (needSwitchChildren) {
      (lkeys, rkeys)
    } else {
      (rkeys, lkeys)
    }
  }

  protected lazy val substraitJoinType: JoinRel.JoinType = SubstraitUtil.toSubstrait(joinType)
  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genHashJoinTransformerMetricsUpdater(metrics)

  override def outputPartitioning: Partitioning = joinBuildSide match {
    case BuildLeft =>
      joinType match {
        case _: InnerLike => expandPartitioning(right.outputPartitioning)
        case RightOuter => right.outputPartitioning
        case LeftOuter => left.outputPartitioning
        case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
        case x =>
          throw new IllegalArgumentException(
            s"HashJoin should not take $x as the JoinType with building left side")
      }
    case BuildRight =>
      joinType match {
        case _: InnerLike => expandPartitioning(left.outputPartitioning)
        case LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin => left.outputPartitioning
        case RightOuter => right.outputPartitioning
        case FullOuter => UnknownPartitioning(right.outputPartitioning.numPartitions)
        case x =>
          throw new IllegalArgumentException(
            s"HashJoin should not take $x as the JoinType with building right side")
      }
  }

  // https://issues.apache.org/jira/browse/SPARK-31869
  private def expandPartitioning(partitioning: Partitioning): Partitioning = {
    val expandLimit = conf.broadcastHashJoinOutputPartitioningExpandLimit
    joinType match {
      case _: InnerLike if expandLimit > 0 =>
        new ExpandOutputPartitioningShim(streamedKeyExprs, buildKeyExprs, expandLimit)
          .expandPartitioning(partitioning)
      case _ => partitioning
    }
  }

  override protected def doValidateInternal(): ValidationResult = {
    val substraitContext = new SubstraitContext
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    if (substraitJoinType == JoinRel.JoinType.UNRECOGNIZED) {
      return ValidationResult
        .notOk(s"Unsupported join type of $hashJoinType for substrait: $substraitJoinType")
    }
    val relNode = JoinUtils.createJoinRel(
      streamedKeyExprs,
      buildKeyExprs,
      condition,
      substraitJoinType,
      needSwitchChildren,
      joinType,
      genJoinParameters(),
      null,
      null,
      streamedPlan.output,
      buildPlan.output,
      substraitContext,
      substraitContext.nextOperatorId(this.nodeName),
      validation = true
    )
    // Then, validate the generated plan in native engine.
    doNativeValidation(substraitContext, relNode)
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val streamedPlanContext = streamedPlan.asInstanceOf[TransformSupport].doTransform(context)
    val (inputStreamedRelNode, inputStreamedOutput) =
      (streamedPlanContext.root, streamedPlanContext.outputAttributes)

    val buildPlanContext = buildPlan.asInstanceOf[TransformSupport].doTransform(context)
    val (inputBuildRelNode, inputBuildOutput) =
      (buildPlanContext.root, buildPlanContext.outputAttributes)

    // Get the operator id of this Join.
    val operatorId = context.nextOperatorId(this.nodeName)

    val joinParams = new JoinParams
    if (JoinUtils.preProjectionNeeded(streamedKeyExprs)) {
      joinParams.streamPreProjectionNeeded = true
    }
    if (JoinUtils.preProjectionNeeded(buildKeyExprs)) {
      joinParams.buildPreProjectionNeeded = true
    }

    if (condition.isDefined) {
      joinParams.isWithCondition = true
    }

    if (this.isInstanceOf[BroadcastHashJoinExecTransformerBase]) {
      joinParams.isBHJ = true
    }

    val joinRel = JoinUtils.createJoinRel(
      streamedKeyExprs,
      buildKeyExprs,
      condition,
      substraitJoinType,
      needSwitchChildren,
      joinType,
      genJoinParameters(),
      inputStreamedRelNode,
      inputBuildRelNode,
      inputStreamedOutput,
      inputBuildOutput,
      context,
      operatorId
    )

    context.registerJoinParam(operatorId, joinParams)

    JoinUtils.createTransformContext(
      needSwitchChildren,
      output,
      joinRel,
      inputStreamedOutput,
      inputBuildOutput)
  }

  def genJoinParameters(): Any = {
    val (isBHJ, isNullAwareAntiJoin, buildHashTableId) = genJoinParametersInternal()
    // Start with "JoinParameters:"
    val joinParametersStr = new StringBuffer("JoinParameters:")
    // isBHJ: 0 for SHJ, 1 for BHJ
    // isNullAwareAntiJoin: 0 for false, 1 for true
    // buildHashTableId: the unique id for the hash table of build plan
    joinParametersStr
      .append("isBHJ=")
      .append(isBHJ)
      .append("\n")
      .append("isNullAwareAntiJoin=")
      .append(isNullAwareAntiJoin)
      .append("\n")
      .append("buildHashTableId=")
      .append(buildHashTableId)
      .append("\n")
      .append("isExistenceJoin=")
      .append(if (joinType.isInstanceOf[ExistenceJoin]) 1 else 0)
      .append("\n")
    val message = StringValue
      .newBuilder()
      .setValue(joinParametersStr.toString)
      .build()
    BackendsApiManager.getTransformerApiInstance.packPBMessage(message)
  }

  def genJoinParametersInternal(): (Int, Int, String) = {
    (0, 0, "")
  }
}

object HashJoinLikeExecTransformer {
  def makeEqualToExpression(
      leftNode: ExpressionNode,
      leftType: DataType,
      rightNode: ExpressionNode,
      rightType: DataType,
      functionMap: JMap[String, JLong]): ExpressionNode = {
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(ExpressionNames.EQUAL, Seq(leftType, rightType)))

    val expressionNodes = Lists.newArrayList(leftNode, rightNode)
    val typeNode = TypeBuilder.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }

  def makeAndExpression(
      leftNode: ExpressionNode,
      rightNode: ExpressionNode,
      functionMap: JMap[String, JLong]): ExpressionNode = {
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(ExpressionNames.AND, Seq(BooleanType, BooleanType)))

    val expressionNodes = Lists.newArrayList(leftNode, rightNode)
    val typeNode = TypeBuilder.makeBoolean(true)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

abstract class ShuffledHashJoinExecTransformerBase(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean)
  extends HashJoinLikeExecTransformer
  with ColumnarShuffledJoin {

  override def joinBuildSide: BuildSide = buildSide
  override def hashJoinType: JoinType = joinType

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    getColumnarInputRDDs(streamedPlan) ++ getColumnarInputRDDs(buildPlan)
  }
}

abstract class BroadcastHashJoinExecTransformerBase(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isNullAwareAntiJoin: Boolean)
  extends HashJoinLikeExecTransformer {

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
        throw new IllegalArgumentException(s"${getClass.getSimpleName} not take $x as the JoinType")
    }
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(buildKeyExprs, isNullAwareAntiJoin)
    buildSide match {
      case BuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case BuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  override def joinBuildSide: BuildSide = buildSide
  override def hashJoinType: JoinType = joinType

  // Unique ID for builded hash table
  lazy val buildHashTableId: String = "BuiltHashTable-" + buildPlan.id

  override def genJoinParametersInternal(): (Int, Int, String) = {
    (1, if (isNullAwareAntiJoin) 1 else 0, buildHashTableId)
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    val streamedRDD = getColumnarInputRDDs(streamedPlan)
    val broadcastRDD = {
      val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
      BackendsApiManager.getBroadcastApiInstance
        .collectExecutionBroadcastTableId(executionId, buildHashTableId)
      createBroadcastBuildSideRDD()
    }
    // FIXME: Do we have to make build side a RDD?
    streamedRDD :+ broadcastRDD
  }

  protected def createBroadcastBuildSideRDD(): BroadcastBuildSideRDD
}
