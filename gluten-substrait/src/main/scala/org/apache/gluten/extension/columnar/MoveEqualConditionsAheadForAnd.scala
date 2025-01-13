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
package org.apache.gluten.extension.columnar

import org.apache.gluten.execution.FilterExecTransformerBase

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Cast, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * rewrite `and` filter conditions, move the simple conditions ahead for example: ... where cast(b
 * as string) != '' and a = 1 => ... where a = and cast(b as string) != ''
 */
object MoveEqualConditionsAheadForAnd extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    moveSimpleConditionsAhead(plan)
  }

  private def moveSimpleConditionsAhead(plan: SparkPlan): SparkPlan = {
    plan match {
      case f: FilterExecTransformerBase if f.cond.isInstanceOf[And] =>
        val and = f.cond.asInstanceOf[And]
        val rightEqualTo = and.right.asInstanceOf[EqualTo]
        logInfo("rightEqualTo.111:" + rightEqualTo.left.getClass.getName)
        val equalRightCast = rightEqualTo.right.asInstanceOf[Cast]
        logInfo("castChild:" + equalRightCast.child.getClass.getName)
        logInfo("castDataType:" + equalRightCast.dataType.getClass.getName)
      case f: FilterExecTransformerBase =>
        logInfo("f.cond.class:" + f.cond.getClass.getName)
    }
    plan
  }

  private def isSimpleCondition(expr: Expression): Boolean = {
    var leftExpr = null.asInstanceOf[Expression]
    var rightExpr = null.asInstanceOf[Expression]
    expr match {
      case e: EqualTo => leftExpr = e.left; rightExpr = e.right
      case e: EqualNullSafe => leftExpr = e.left; rightExpr = e.right
      case l: LessThan => leftExpr = l.left; rightExpr = l.right
      case l: LessThanOrEqual => leftExpr = l.left; rightExpr = l.right
      case g: GreaterThan => leftExpr = g.left; rightExpr = g.right
      case g: GreaterThanOrEqual => leftExpr = g.left; rightExpr = g.right
      case _ =>
        return false
    }
    if (leftExpr == null || rightExpr == null)
      return false

    leftExpr match {
      case _: Attribute => {
        rightExpr match {
          case _: Literal =>
            return true
          case c: Cast if c.child.isInstanceOf[Literal] =>
            return true
          case _ =>
        }
      }
    }
    false
  }
}
