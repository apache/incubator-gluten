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
package io.glutenproject.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, GreaterThan, LessThan, Not, Or}
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.GreaterThanOrEqual
import org.apache.spark.sql.catalyst.expressions.LessThanOrEqual

object CHJoinValidateUtil extends Logging {
  def hasTwoTableColumn(l: Expression, r: Expression): Boolean = {
    !l.references.toSeq
      .map(_.qualifier.mkString("."))
      .toSet
      .subsetOf(r.references.toSeq.map(_.qualifier.mkString(".")).toSet) && !r.references.nonEmpty
  }

  // Rules:
  // 1. Only one inequality condition is allowed in clickhouse asof join, e.g. <, <=, >, >=
  // 2. Clickhouse backend do not support or/not expressions as join condition
  def shouldFallback(condition: Option[Expression]): Boolean = {
    val (fallback, inequalities) = shouldFallBackInteral(condition)
    fallback || inequalities > 1
  }

  def shouldFallBackInteral(condition: Option[Expression]): (Boolean, Int) = {
    if (condition.isEmpty) {
      return (false, 0)
    }

    condition.get match {
      case And(l, r) =>
        val (leftFallback, leftInequalities) = shouldFallBackInteral(Some(l))
        val (rightFallback, rightInequalities) = shouldFallBackInteral(Some(r))
        if (leftFallback || rightFallback || leftInequalities + rightInequalities > 1) {
          (true, 0)
        } else {
          (false, leftInequalities + rightInequalities)
        }
      case LessThan(l, r) =>
        if (hasTwoTableColumn(l, r)) {
          (false, 1)
        } else {
          (false, 0)
        }
      case LessThanOrEqual(l, r) =>
        if (hasTwoTableColumn(l, r)) {
          (false, 1)
        } else {
          (false, 0)
        }
      case GreaterThan(l, r) =>
        if (hasTwoTableColumn(l, r)) {
          (false, 1)
        } else {
          (false, 0)
        }
      case GreaterThanOrEqual(l, r) =>
        if (hasTwoTableColumn(l, r)) {
          (false, 1)
        } else {
          (false, 0)
        }
      case _: Expression =>
        (true, 0)
    }
  }
}
