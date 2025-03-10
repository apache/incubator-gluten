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

import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.GlutenDriverEndpoint
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.{BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, JoinType}
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.vectorized.ColumnarBatch

case class VeloxBroadcastNestedLoopJoinExecTransformer(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    condition: Option[Expression])
  extends BroadcastNestedLoopJoinExecTransformer(
    left,
    right,
    buildSide,
    joinType,
    condition
  ) {

  override def columnarInputRDDs: Seq[RDD[ColumnarBatch]] = {
    val streamedRDD = getColumnarInputRDDs(streamedPlan)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionId != null) {
      GlutenDriverEndpoint.collectResources(executionId, buildBroadcastTableId)
    } else {
      logWarning(
        s"Can't not trace broadcast table data $buildBroadcastTableId" +
          s" because execution id is null." +
          s" Will clean up until expire time.")
    }

    val broadcast = buildPlan.executeBroadcast[BuildSideRelation]()
    val context =
      BroadCastHashJoinContext(
        Seq.empty,
        joinType,
        buildSide == BuildRight,
        false,
        joinType.isInstanceOf[ExistenceJoin],
        buildPlan.output,
        buildBroadcastTableId)
    val broadcastRDD = VeloxBroadcastBuildSideRDD(sparkContext, broadcast, context)
    // FIXME: Do we have to make build side a RDD?
    streamedRDD :+ broadcastRDD
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): VeloxBroadcastNestedLoopJoinExecTransformer =
    copy(left = newLeft, right = newRight)

}
