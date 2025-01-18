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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{FilterExecTransformer, ProjectExecTransformer}
import org.apache.gluten.expression.ExpressionMappings

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Expression, GetStructField, Literal, NamedExpression, ScalaUDF}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{DataType, DataTypes}

case class CollapseNestedExpressions(spark: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (GlutenConfig.get.enableCollapseNestedFunctions) {
      val p = visitPlan(plan)
      p
    } else {
      plan
    }
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
    var resultExpr = null.asInstanceOf[Expression]
    var name = Option.empty[String]
    var children = Seq.empty[Expression]
    var nestedFunctions = 0
    var dataType = null.asInstanceOf[DataType]
    def f(e: Expression, nested: Boolean = false): Unit = e match {
      case g: GetStructField =>
        if (!nested) {
          name = ExpressionMappings.expressionsMap.get(classOf[GetStructField])
          dataType = g.dataType
        }
        children +:= Literal.apply(g.ordinal, DataTypes.IntegerType)
        f(g.child, nested = true)
        nestedFunctions += 1
      case a: And =>
        if (!nested) {
          name = ExpressionMappings.expressionsMap.get(classOf[And])
          dataType = a.dataType
        }
        f(a.left, nested = true)
        f(a.right, nested = true)
        nestedFunctions += 1
      case _ =>
        if (nested) {
          children +:= e
        } else {
          nestedFunctions = 0
          children = Seq.empty[Expression]
          val exprNewChildren = e.children.map(p => optimize(p))
          resultExpr = e.withNewChildren(exprNewChildren)
        }
    }
    f(expr)
    if (nestedFunctions > 1 && name.isDefined && dataType != null) {
      val func: Null = null
      ScalaUDF(func, dataType, children, udfName = name, nullable = expr.nullable)
    } else {
      resultExpr
    }
  }
}
