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
package org.apache.gluten.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression}
import org.apache.spark.sql.catalyst.plans.JoinType

trait JoinStrategy {
  val joinType: JoinType
}
case class UnknownJoinStrategy(joinType: JoinType) extends JoinStrategy {}
case class ShuffleHashJoinStrategy(joinType: JoinType) extends JoinStrategy {}
case class BroadcastHashJoinStrategy(joinType: JoinType) extends JoinStrategy {}
case class SortMergeJoinStrategy(joinType: JoinType) extends JoinStrategy {}

/**
 * The logic here is that if it is not an equi-join spark will create BNLJ, which will fallback, if
 * it is an equi-join, spark will create BroadcastHashJoin or ShuffleHashJoin, for these join types,
 * we need to filter For cases that cannot be handled by the backend, 1 there are at least two
 * different tables column and Literal in the condition Or condition for comparison, for example: (a
 * join b on a.a1 = b.b1 and (a.a2 > 1 or b.b2 < 2) ) 2 tow join key for inequality comparison (!= ,
 * > , <), for example: (a join b on a.a1 > b.b1) There will be a fallback for Nullaware Jion For
 * Existence Join which is just an optimization of exist subquery, it will also fallback
 */

object CHJoinValidateUtil extends Logging {
  def hasTwoTableColumn(
      leftOutputSet: AttributeSet,
      rightOutputSet: AttributeSet,
      expr: Expression): Boolean = {
    val allReferences = expr.references
    !(allReferences.subsetOf(leftOutputSet) || allReferences.subsetOf(rightOutputSet))
  }

  def shouldFallback(
      joinStrategy: JoinStrategy,
      leftOutputSet: AttributeSet,
      rightOutputSet: AttributeSet,
      condition: Option[Expression]): Boolean = {
    var shouldFallback = false
    val joinType = joinStrategy.joinType
    if (joinType.toString.contains("ExistenceJoin")) {
      logError("Fallback for join type ExistenceJoin")
      return true
    }
    if (joinType.sql.contains("INNER")) {
      shouldFallback = false;
    } else if (
      condition.isDefined && hasTwoTableColumn(leftOutputSet, rightOutputSet, condition.get)
    ) {
      shouldFallback = joinStrategy match {
        case BroadcastHashJoinStrategy(joinTy) =>
          joinTy.sql.contains("SEMI") || joinTy.sql.contains("ANTI")
        case SortMergeJoinStrategy(_) => true
        case ShuffleHashJoinStrategy(joinTy) =>
          joinTy.sql.contains("SEMI") || joinTy.sql.contains("ANTI")
        case UnknownJoinStrategy(joinTy) =>
          joinTy.sql.contains("SEMI") || joinTy.sql.contains("ANTI")
      }
    } else {
      shouldFallback = joinStrategy match {
        case SortMergeJoinStrategy(joinTy) =>
          joinTy.sql.contains("SEMI") || joinTy.sql.contains("ANTI")
        case _ => false
      }
    }
    if (shouldFallback) {
      logError(s"Fallback for join type $joinType")
    }
    shouldFallback
  }
}
