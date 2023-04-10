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

import io.glutenproject.utils.CHJoinValidateUtil

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.SparkPlan

import io.substrait.proto.JoinRel

case class CHShuffledHashJoinExecTransformer(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isSkewJoin: Boolean)
  extends ShuffledHashJoinExecTransformer(
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

  override def doValidateInternal(): Boolean = {
    var shouldFallback = false
    if (substraitJoinType != JoinRel.JoinType.JOIN_TYPE_INNER) {
      shouldFallback = CHJoinValidateUtil.doValidate(condition)
    }
    if (shouldFallback) {
      return false
    }
    super.doValidateInternal()
  }
}

case class CHBroadcastHashJoinExecTransformer(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isNullAwareAntiJoin: Boolean)
  extends BroadcastHashJoinExecTransformer(
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

  /*

   */
  override def doValidateInternal(): Boolean = {
    var shouldFallback = false
    if (substraitJoinType != JoinRel.JoinType.JOIN_TYPE_INNER) {
      shouldFallback = CHJoinValidateUtil.doValidate(condition)
    }
    if (isNullAwareAntiJoin == true) {
      shouldFallback = true
    }
    if (shouldFallback) {
      return false
    }
    super.doValidateInternal()
  }
}
