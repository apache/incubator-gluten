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

import org.apache.gluten.execution.{FilterExecTransformer, ProjectExecTransformer}
import org.apache.gluten.expression.{CHFlattenedExpression, ExpressionMappings}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DataType

/**
 * Flatten nested expressions for optimization, to reduce expression calls. Now support `and`, `or`.
 * e.g. select ... and(and(a=1, b=2), c=3) => select ... and(a=1, b=2, c=3).
 */
case class FlattenNestedExpressions(spark: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (canBeOptimized(plan)) {
      visitPlan(plan)
    } else {
      plan
    }
  }

  private def canBeOptimized(plan: SparkPlan): Boolean = plan match {
    case p: ProjectExecTransformer =>
      var res = p.projectList.exists(c => c.isInstanceOf[And] || c.isInstanceOf[Or])
      if (res) {
        return false
      }
      res = p.projectList.exists(c => canBeOptimized(c))
      if (!res) {
        res = p.children.exists(c => canBeOptimized(c))
      }
      res
    case f: FilterExecTransformer =>
      var res = canBeOptimized(f.condition)
      if (!res) {
        res = canBeOptimized(f.child)
      }
      res
    case _ => plan.children.exists(c => canBeOptimized(c))
  }

  private def canBeOptimized(expr: Expression): Boolean = {
    var exprCall = expr
    expr match {
      case a: Alias => exprCall = a.child
      case _ =>
    }
    val exprName = getExpressionName(exprCall)
    exprName match {
      case None =>
        exprCall match {
          case _: LeafExpression => false
          case _ => exprCall.children.exists(c => canBeOptimized(c))
        }
      case Some(f) =>
        CHFlattenedExpression.supported(f)
    }
  }

  private def getExpressionName(expr: Expression): Option[String] = expr match {
    case _: And => ExpressionMappings.expressionsMap.get(classOf[And])
    case _: Or => ExpressionMappings.expressionsMap.get(classOf[Or])
    case _ => Option.empty[String]
  }

  private def visitPlan(plan: SparkPlan): SparkPlan = plan match {
    case p: ProjectExecTransformer =>
      var newProjectList = Seq.empty[NamedExpression]
      p.projectList.foreach {
        case a: Alias =>
          val newAlias = Alias(optimize(a.child), a.name)(a.exprId)
          newProjectList :+= newAlias
        case p =>
          newProjectList :+= p
      }
      val newChild = visitPlan(p.child)
      ProjectExecTransformer(newProjectList, newChild)
    case f: FilterExecTransformer =>
      val newCondition = optimize(f.condition)
      val newChild = visitPlan(f.child)
      FilterExecTransformer(newCondition, newChild)
    case _ =>
      val newChildren = plan.children.map(p => visitPlan(p))
      plan.withNewChildren(newChildren)
  }

  private def optimize(expr: Expression): Expression = {
    var resultExpr = expr
    var name = getExpressionName(expr)
    var children = Seq.empty[Expression]
    var dataType = null.asInstanceOf[DataType]
    var nestedFunctions = 0

    def f(e: Expression, parent: Option[Expression] = Option.empty[Expression]): Unit = {
      parent match {
        case None =>
          name = getExpressionName(e)
          dataType = e.dataType
        case _ =>
      }
      e match {
        case a: And if canBeOptimized(a) =>
          parent match {
            case Some(_: And) | None =>
              f(a.left, Option.apply(a))
              f(a.right, Option.apply(a))
              nestedFunctions += 1
            case _ =>
              children +:= optimize(a)
          }
        case o: Or if canBeOptimized(o) =>
          parent match {
            case Some(_: Or) | None =>
              f(o.left, parent = Option.apply(o))
              f(o.right, parent = Option.apply(o))
              nestedFunctions += 1
            case _ =>
              children +:= optimize(o)
          }
        case _ =>
          if (parent.nonEmpty) {
            children +:= optimize(e)
          } else {
            children = Seq.empty[Expression]
            nestedFunctions = 0
            val exprNewChildren = e.children.map(p => optimize(p))
            resultExpr = e.withNewChildren(exprNewChildren)
          }
      }
    }
    f(expr)
    if ((nestedFunctions > 1 && name.isDefined) || flattenedExpressionExists(children)) {
      CHFlattenedExpression.genFlattenedExpression(
        dataType,
        children,
        name.getOrElse(""),
        expr.nullable) match {
        case Some(f) => f
        case None => resultExpr
      }
    } else {
      resultExpr
    }
  }

  private def flattenedExpressionExists(children: Seq[Expression]): Boolean = {
    var res = false
    children.foreach {
      case _: CHFlattenedExpression if !res => res = true
      case c if !res => res = flattenedExpressionExists(c.children)
      case _ =>
    }
    res
  }
}
