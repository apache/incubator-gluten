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

import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.utils.{BroadcastHashJoinStrategy, CHJoinValidateUtil, ShuffleHashJoinStrategy}

import org.apache.spark.{broadcast, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.GlutenDriverEndpoint
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

import io.substrait.proto.JoinRel

object JoinTypeTransform {
  def toNativeJoinType(joinType: JoinType): JoinType = {
    joinType match {
      case ExistenceJoin(_) =>
        LeftSemi
      case _ =>
        joinType
    }
  }

  def toSubstraitType(joinType: JoinType): JoinRel.JoinType = {
    joinType match {
      case _: InnerLike =>
        JoinRel.JoinType.JOIN_TYPE_INNER
      case FullOuter =>
        JoinRel.JoinType.JOIN_TYPE_OUTER
      case LeftOuter | RightOuter =>
        JoinRel.JoinType.JOIN_TYPE_LEFT
      case LeftSemi | ExistenceJoin(_) =>
        JoinRel.JoinType.JOIN_TYPE_LEFT_SEMI
      case LeftAnti =>
        JoinRel.JoinType.JOIN_TYPE_ANTI
      case _ =>
        // TODO: Support cross join with Cross Rel
        JoinRel.JoinType.UNRECOGNIZED
    }
  }
}

case class CHShuffledHashJoinExecTransformer(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean)
  extends ShuffledHashJoinExecTransformerBase(
    leftKeys,
    rightKeys,
    joinType,
    buildSide,
    condition,
    left,
    right,
    isSkewJoin) {
  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): CHShuffledHashJoinExecTransformer =
    copy(left = newLeft, right = newRight)

  override protected def doValidateInternal(): ValidationResult = {
    val shouldFallback =
      CHJoinValidateUtil.shouldFallback(
        ShuffleHashJoinStrategy(finalJoinType),
        left.outputSet,
        right.outputSet,
        condition)
    if (shouldFallback) {
      return ValidationResult.failed("ch join validate fail")
    }
    super.doValidateInternal()
  }
  private val finalJoinType = JoinTypeTransform.toNativeJoinType(joinType)
  override protected lazy val substraitJoinType: JoinRel.JoinType =
    JoinTypeTransform.toSubstraitType(joinType)
}

case class CHBroadcastBuildSideRDD(
    @transient private val sc: SparkContext,
    broadcasted: broadcast.Broadcast[BuildSideRelation],
    broadcastContext: BroadCastHashJoinContext)
  extends BroadcastBuildSideRDD(sc, broadcasted) {

  override def genBroadcastBuildSideIterator(): Iterator[ColumnarBatch] = {
    CHBroadcastBuildSideCache.getOrBuildBroadcastHashTable(broadcasted, broadcastContext)
    Iterator.empty
  }
}

case class BroadCastHashJoinContext(
    buildSideJoinKeys: Seq[Expression],
    joinType: JoinType,
    hasMixedFiltCondition: Boolean,
    isExistenceJoin: Boolean,
    buildSideStructure: Seq[Attribute],
    buildHashTableId: String)

case class CHBroadcastHashJoinExecTransformer(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isNullAwareAntiJoin: Boolean)
  extends BroadcastHashJoinExecTransformerBase(
    leftKeys,
    rightKeys,
    joinType,
    buildSide,
    condition,
    left,
    right,
    isNullAwareAntiJoin) {

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): CHBroadcastHashJoinExecTransformer =
    copy(left = newLeft, right = newRight)

  override protected def doValidateInternal(): ValidationResult = {
    val shouldFallback =
      CHJoinValidateUtil.shouldFallback(
        BroadcastHashJoinStrategy(finalJoinType),
        left.outputSet,
        right.outputSet,
        condition)

    if (shouldFallback) {
      return ValidationResult.failed("ch join validate fail")
    }
    if (isNullAwareAntiJoin) {
      return ValidationResult.failed("ch does not support NAAJ")
    }
    super.doValidateInternal()
  }

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    val streamedRDD = getColumnarInputRDDs(streamedPlan)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionId != null) {
      GlutenDriverEndpoint.collectResources(executionId, buildHashTableId)
    } else {
      logWarning(
        s"Can't not trace broadcast hash table data $buildHashTableId" +
          s" because execution id is null." +
          s" Will clean up until expire time.")
    }
    val broadcast = buildPlan.executeBroadcast[BuildSideRelation]()
    val context =
      BroadCastHashJoinContext(
        buildKeyExprs,
        finalJoinType,
        isMixedCondition(condition),
        joinType.isInstanceOf[ExistenceJoin],
        buildPlan.output,
        buildHashTableId)
    val broadcastRDD = CHBroadcastBuildSideRDD(sparkContext, broadcast, context)
    // FIXME: Do we have to make build side a RDD?
    streamedRDD :+ broadcastRDD
  }

  def isMixedCondition(cond: Option[Expression]): Boolean = {
    val res = if (cond.isDefined) {
      val leftOutputSet = left.outputSet
      val rightOutputSet = right.outputSet
      val allReferences = cond.get.references
      !(allReferences.subsetOf(leftOutputSet) || allReferences.subsetOf(rightOutputSet))
    } else {
      false
    }
    res
  }

  // ExistenceJoin is introduced in #SPARK-14781. It returns all rows from the left table with
  // a new column to indecate whether the row is matched in the right table.
  // Indeed, the ExistenceJoin is transformed into left any join in CH.
  // We don't have left any join in substrait, so use left semi join instead.
  // and isExistenceJoin is set to true to indicate that it is an existence join.
  private val finalJoinType = JoinTypeTransform.toNativeJoinType(joinType)
  override protected lazy val substraitJoinType: JoinRel.JoinType =
    JoinTypeTransform.toSubstraitType(joinType)
}
