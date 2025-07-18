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
import org.apache.spark.sql.catalyst.plans._

trait JoinStrategy {
  val joinType: JoinType
}
case class UnknownJoinStrategy(joinType: JoinType) extends JoinStrategy {}
case class ShuffleHashJoinStrategy(joinType: JoinType) extends JoinStrategy {}
case class BroadcastHashJoinStrategy(joinType: JoinType) extends JoinStrategy {}
case class SortMergeJoinStrategy(joinType: JoinType) extends JoinStrategy {}

/**
 * BroadcastHashJoinStrategy and ShuffleHashJoinStrategy are relatively complete, They support
 * left/right/inner full/anti/semi join, existence Join, and also support join contiditions with
 * columns from both sides. e.g. (a join b on a.a1 = b.b1 and a.a2 > 1 and b.b2 < 2)
 * SortMergeJoinStrategy is not fully supported for all cases in CH.
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

    val hasMixedFilterCondition =
      condition.isDefined && hasTwoTableColumn(leftOutputSet, rightOutputSet, condition.get)
    val shouldFallback = joinStrategy match {
      case SortMergeJoinStrategy(joinType) =>
        if (!joinType.isInstanceOf[ExistenceJoin] && joinType.sql.contains("INNER")) {
          false
        } else {
          joinType.sql.contains("SEMI") || joinType.sql.contains("ANTI") || joinType.toString
            .contains("ExistenceJoin") || hasMixedFilterCondition
        }
      case UnknownJoinStrategy(joinType) =>
        throw new IllegalArgumentException(s"Unknown join type $joinStrategy")
      case _ => false
    }

    if (shouldFallback) {
      logError(s"Fallback for join type $joinStrategy")
    }
    shouldFallback
  }
}
