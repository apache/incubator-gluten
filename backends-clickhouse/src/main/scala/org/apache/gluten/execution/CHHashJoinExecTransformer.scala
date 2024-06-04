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

import org.apache.gluten.backendsapi.clickhouse.CHIteratorApi
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
        ShuffleHashJoinStrategy(joinType),
        left.outputSet,
        right.outputSet,
        condition)
    if (shouldFallback) {
      return ValidationResult.notOk("ch join validate fail")
    }
    super.doValidateInternal()
  }
}

case class CHBroadcastBuildSideRDD(
    @transient private val sc: SparkContext,
    broadcasted: broadcast.Broadcast[BuildSideRelation],
    broadcastContext: BroadCastHashJoinContext)
  extends BroadcastBuildSideRDD(sc, broadcasted) {

  override def genBroadcastBuildSideIterator(): Iterator[ColumnarBatch] = {
    CHBroadcastBuildSideCache.getOrBuildBroadcastHashTable(broadcasted, broadcastContext)
    CHIteratorApi.genCloseableColumnBatchIterator(Iterator.empty)
  }
}

case class BroadCastHashJoinContext(
    buildSideJoinKeys: Seq[Expression],
    joinType: JoinType,
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
        BroadcastHashJoinStrategy(joinType),
        left.outputSet,
        right.outputSet,
        condition)

    if (shouldFallback) {
      return ValidationResult.notOk("ch join validate fail")
    }
    if (isNullAwareAntiJoin) {
      return ValidationResult.notOk("ch does not support NAAJ")
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
      BroadCastHashJoinContext(buildKeyExprs, joinType, buildPlan.output, buildHashTableId)
    val broadcastRDD = CHBroadcastBuildSideRDD(sparkContext, broadcast, context)
    // FIXME: Do we have to make build side a RDD?
    streamedRDD :+ broadcastRDD
  }
}
