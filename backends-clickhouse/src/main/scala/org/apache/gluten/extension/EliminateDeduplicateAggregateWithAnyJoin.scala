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
package org.apache.gluten.extension

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.execution._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

case class EliminateDeduplicateAggregateWithAnyJoin(spark: SparkSession)
  extends Rule[SparkPlan]
  with Logging {
  override def apply(plan: SparkPlan): SparkPlan = {

    if (
      !spark.conf
        .get(CHBackendSettings.GLUTEN_ELIMINATE_DEDUPLICATE_AGGREGATE_WITH_ANY_JOIN, "true")
        .toBoolean
    ) {
      return plan
    }

    plan.transformUp {
      case hashJoin: CHShuffledHashJoinExecTransformer
          if (hashJoin.buildSide == BuildRight && hashJoin.joinType == LeftOuter) =>
        hashJoin.right match {
          case aggregate: CHHashAggregateExecTransformer =>
            if (
              isDeduplicateAggregate(aggregate) && allGroupingKeysAreJoinKeys(hashJoin, aggregate)
            ) {
              hashJoin.copy(right = aggregate.child, isAnyJoin = true)
            } else {
              hashJoin
            }
          case project @ ProjectExecTransformer(_, aggregate: CHHashAggregateExecTransformer) =>
            if (
              hashJoin.joinType == LeftOuter &&
              isDeduplicateAggregate(aggregate) &&
              allGroupingKeysAreJoinKeys(hashJoin, aggregate) && project.projectList.forall(
                _.isInstanceOf[AttributeReference])
            ) {
              hashJoin.copy(right = project.copy(child = aggregate.child), isAnyJoin = true)
            } else {
              hashJoin
            }
          case _ => hashJoin
        }
      case hashJoin: CHShuffledHashJoinExecTransformer
          if (hashJoin.buildSide == BuildLeft && hashJoin.joinType == LeftOuter) =>
        hashJoin.left match {
          case aggregate: CHHashAggregateExecTransformer =>
            if (
              isDeduplicateAggregate(aggregate) && allGroupingKeysAreJoinKeys(hashJoin, aggregate)
            ) {
              hashJoin.copy(left = aggregate.child, isAnyJoin = true)
            } else {
              hashJoin
            }
          case project @ ProjectExecTransformer(_, aggregate: CHHashAggregateExecTransformer) =>
            if (
              hashJoin.joinType == LeftOuter &&
              isDeduplicateAggregate(aggregate) &&
              allGroupingKeysAreJoinKeys(hashJoin, aggregate) && project.projectList.forall(
                _.isInstanceOf[AttributeReference])
            ) {
              hashJoin.copy(left = project.copy(child = aggregate.child), isAnyJoin = true)
            } else {
              hashJoin
            }
          case _ => hashJoin
        }
    }
  }

  def isDeduplicateAggregate(aggregate: CHHashAggregateExecTransformer): Boolean = {
    aggregate.aggregateExpressions.isEmpty && aggregate.groupingExpressions.forall(
      _.isInstanceOf[AttributeReference])
  }

  def allGroupingKeysAreJoinKeys(
      join: CHShuffledHashJoinExecTransformer,
      aggregate: CHHashAggregateExecTransformer): Boolean = {
    val rightKeys = join.rightKeys
    val groupingKeys = aggregate.groupingExpressions
    groupingKeys.forall(key => rightKeys.exists(_.semanticEquals(key))) &&
    groupingKeys.length == rightKeys.length
  }
}
