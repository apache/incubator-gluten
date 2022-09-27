/*
 * Copyright 2020 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.glutenproject.sql.shims.spark33

import io.glutenproject.extension.JoinSelectionOverrideShim
import io.glutenproject.sql.shims.{ShimDescriptor, SparkShims}
import io.glutenproject.BackendLib

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution}
import org.apache.spark.sql.execution.SparkPlan

class Spark33Shims extends SparkShims {
  override def getShimDescriptor: ShimDescriptor = SparkShimProvider.DESCRIPTOR

  override def getDistribution(leftKeys: Seq[Expression], rightKeys: Seq[Expression])
  : Seq[Distribution] = {
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil
  }

  override def applyPlan(plan: LogicalPlan,
                         forceShuffledHashJoin: Boolean,
                         backendLib: BackendLib): Seq[SparkPlan] = {
    plan match {
      // If the build side of BHJ is already decided by AQE, we need to keep the build side.
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, _, left, right, hint) =>
        new JoinSelectionOverrideShim(backendLib).extractEqualJoinKeyCondition(
          joinType, leftKeys, rightKeys, condition, left, right, hint, forceShuffledHashJoin)
      case _ => Nil
    }
  }
}
