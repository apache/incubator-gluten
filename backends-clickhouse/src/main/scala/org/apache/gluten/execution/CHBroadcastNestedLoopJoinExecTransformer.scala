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

import org.apache.spark.rdd.RDD
import org.apache.spark.rpc.GlutenDriverEndpoint
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.execution.{SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.joins.{BuildSideRelation, HashJoin}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import com.google.protobuf.{Any, StringValue}

case class CHBroadcastNestedLoopJoinExecTransformer(
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
  // Unique ID for builded table
  lazy val buildBroadcastTableId: String = "BuiltBroadcastTable-" + buildPlan.id

  lazy val (buildKeyExprs, streamedKeyExprs) = {
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
      BroadCastHashJoinContext(Seq.empty, joinType, false, buildPlan.output, buildBroadcastTableId)
    val broadcastRDD = CHBroadcastBuildSideRDD(sparkContext, broadcast, context)
    // FIXME: Do we have to make build side a RDD?
    streamedRDD :+ broadcastRDD
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): CHBroadcastNestedLoopJoinExecTransformer =
    copy(left = newLeft, right = newRight)

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

  override def genJoinParameters(): Any = {
    val joinParametersStr = new StringBuffer("JoinParameters:")
    joinParametersStr
      .append("buildHashTableId=")
      .append(buildBroadcastTableId)
      .append("\n")
    val message = StringValue
      .newBuilder()
      .setValue(joinParametersStr.toString)
      .build()
    BackendsApiManager.getTransformerApiInstance.packPBMessage(message)
  }

}
