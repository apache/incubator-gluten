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
import scala.util.control.Breaks.{break, breakable}

import com.google.common.collect.Lists
import com.google.protobuf.{Any, ByteString}
import io.glutenproject.GlutenConfig
import io.glutenproject.execution.HashJoinLikeExecTransformer.{makeAndExpression, makeEqualToExpression}
import io.glutenproject.expression._
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.`type`.{TypeBuilder, TypeNode}
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import io.glutenproject.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.{RelBuilder, RelNode}
import io.glutenproject.vectorized.ExpressionEvaluator
import io.substrait.proto.JoinRel
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{BaseJoinExec, BuildSideRelation, HashJoin, ShuffledJoin}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{BooleanType, DataType, NullType}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
abstract class HashJoinLikeExecTransformer(
                                            leftKeys: Seq[Expression],
                                            rightKeys: Seq[Expression],
                                            joinType: JoinType,
                                            buildSide: BuildSide,
                                            condition: Option[Expression],
                                            left: SparkPlan,
                                            right: SparkPlan)
  extends BaseJoinExec
    with TransformSupport
    with ShuffledJoin {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numOutputBatches" -> SQLMetrics.createMetric(sparkContext, "number of output batches"),
    "processTime" -> SQLMetrics.createTimingMetric(sparkContext, "totaltime_hashjoin"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"),
    "joinTime" -> SQLMetrics.createTimingMetric(sparkContext, "join time"))

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

  val numOutputBatches: SQLMetric = longMetric("numOutputBatches")
  val numOutputRows: SQLMetric = longMetric("numOutputRows")
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
    numOutputBatches += outNumBatches
    numOutputRows += outNumRows
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
    val relNode =
      try {
        createJoinRel(null, null,
          streamedPlan.output, buildPlan.output, substraitContext, validation = true)
      } catch {
        case e: Throwable =>
          logDebug(s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}")
          return false
      }
    // Then, validate the generated plan in native engine.
    if (GlutenConfig.getConf.enableNativeValidation) {
      val validator = new ExpressionEvaluator()
      val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
      val result = validator.doValidate(planNode.toProtobuf.toByteArray)
      result
    } else {
      true
    }
  }

  override def doTransform(substraitContext: SubstraitContext): TransformContext = {
    def transformAndGetOutput(plan: SparkPlan): (RelNode, Seq[Attribute]) = {
      plan match {
        case p: TransformSupport =>
          val transformContext = p.doTransform(substraitContext)
          (transformContext.root, transformContext.outputAttributes)
        case _ =>
          val readRel =
            RelBuilder.makeReadRel(
              new util.ArrayList[Attribute](plan.output.asJava),
              substraitContext)
          (readRel, plan.output)
      }
    }

    val (inputStreamedRelNode, inputStreamedOutput) = transformAndGetOutput(streamedPlan)
    val (inputBuildRelNode, inputBuildOutput) = transformAndGetOutput(buildPlan)
    val joinRel = createJoinRel(
      inputStreamedRelNode,
      inputBuildRelNode,
      inputStreamedOutput,
      inputBuildOutput,
      substraitContext)
    createTransformContext(joinRel, inputStreamedOutput, inputBuildOutput)
  }

  private def createPreProjectionIfNeeded(
                                           keyExprs: Seq[Expression],
                                           inputNode: RelNode,
                                           inputNodeOutput: Seq[Attribute],
                                           joinOutput: Seq[Attribute],
                                           substraitContext: SubstraitContext,
                                           validation: Boolean
                                         )
  : (Seq[(ExpressionNode, DataType)], RelNode, Seq[Attribute]) = {
    if (keyExprs.forall(_.isInstanceOf[AttributeReference])) {
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
        createExtensionNode(inputNodeOutput, validation))

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


  private def createJoinRel(
                             inputStreamedRelNode: RelNode,
                             inputBuildRelNode: RelNode,
                             inputStreamedOutput: Seq[Attribute],
                             inputBuildOutput: Seq[Attribute],
                             substraitContext: SubstraitContext,
                             validation: Boolean = false
                           ): RelNode = {
    // Create pre-projection for build/streamed plan. Append projected keys to each side.
    val (streamedKeys, streamedRelNode, streamedOutput) = createPreProjectionIfNeeded(
      streamedKeyExprs,
      inputStreamedRelNode,
      inputStreamedOutput,
      inputStreamedOutput,
      substraitContext,
      validation)
    val (buildKeys, buildRelNode, buildOutput) = createPreProjectionIfNeeded(
      buildKeyExprs,
      inputBuildRelNode,
      inputBuildOutput,
      streamedOutput ++ inputBuildOutput,
      substraitContext,
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
      createJoinExtensionNode(streamedOutput ++ buildOutput, validation))

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
      createExtensionNode(streamedOutput ++ buildOutput, validation)
    )
  }

  private def createTransformContext(
                                      rel: RelNode,
                                      inputStreamedOutput: Seq[Attribute],
                                      inputBuildOutput: Seq[Attribute]
                                    ): TransformContext = {
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

  private def createExtensionNode(
                                   output: Seq[Attribute],
                                   validation: Boolean
                                 ): AdvancedExtensionNode = {
    // Use field [enhancement] in a extension node for input type validation.
    if (validation) {
      ExtensionBuilder.makeAdvancedExtension(
        createEnhancementForValidation(output))
    } else {
      null
    }
  }

  private def createJoinExtensionNode(
                                       output: Seq[Attribute],
                                       validation: Boolean
                                     ): AdvancedExtensionNode = {
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
      .setValue(ByteString.copyFromUtf8(joinParametersStr.toString()))
      .setTypeUrl("/google.protobuf.StringValue")
  }

  def genJoinParameters(): (Int, Int, String) = {
    (0, 0, "")
  }

  // The output of result projection should be consistent with ShuffledJoin.output
  private def getResultProjectionOutput(
                                         leftOutput: Seq[Attribute],
                                         rightOutput: Seq[Attribute]
                                       ): (Seq[Attribute], Seq[Attribute]) = {
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

case class ShuffledHashJoinExecTransformer(
                                            leftKeys: Seq[Expression],
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
}

case class BroadCastHashJoinContext(buildSideJoinKeys: Seq[Expression],
                                    joinType: JoinType,
                                    buildSideStructure: Seq[Attribute],
                                    buildHashTableId: String)

case class BroadcastHashJoinExecTransformer(
                                             leftKeys: Seq[Expression],
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
}
