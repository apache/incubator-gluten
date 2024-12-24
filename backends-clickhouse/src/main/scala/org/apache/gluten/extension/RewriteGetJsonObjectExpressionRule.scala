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

import org.apache.gluten.GlutenConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.StringType

class RewriteGetJsonObjectExpressionRule(spark: SparkSession) extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (
      plan.resolved
      && GlutenConfig.getConf.enableGluten
      && GlutenConfig.getConf.enableRewriteGetJsonObject
    ) {
      val newPlan = visitPlan(plan)
      newPlan
    } else {
      plan
    }
  }

  private def visitPlan(plan: LogicalPlan): LogicalPlan = plan match {
    case p: Project =>
      var newProjectList = Seq.empty[NamedExpression]
      p.projectList.foreach {
        case a: Alias if a.child.isInstanceOf[GetJsonObject] =>
          newProjectList :+= optimizeNestedFunctionCalls(a).asInstanceOf[NamedExpression]
        case p =>
          newProjectList :+= p
      }
      val newChild = visitPlan(p.child)
      Project(newProjectList, newChild)
    case f: Filter =>
      val newCond = optimizeNestedFunctionCalls(f.condition)
      val newChild = visitPlan(f.child)
      Filter(newCond, newChild)
    case other =>
      val children = other.children.map(visitPlan)
      plan.withNewChildren(children)
  }

  private def optimizeNestedFunctionCalls(
      expr: Expression,
      path: String = "",
      isNested: Boolean = false): Expression = {

    def getPath(expr: Expression): String = expr match {
      case l: Literal if l.dataType.isInstanceOf[StringType] =>
        l.value.toString
      case _ =>
        throw new IllegalArgumentException(
          "The path of get_json_object function must be const string")
    }

    expr match {
      case g: GetJsonObject =>
        val gPath = getPath(g.path).replace("$", "")
        val newPath = gPath + path
        optimizeNestedFunctionCalls(g.json, newPath, isNested = true)
      case _ =>
        val newChildren = expr.children.map(x => optimizeNestedFunctionCalls(x, path))
        val newExpr = expr.withNewChildren(newChildren)
        if (isNested) {
          val pathExpr = Literal.apply("$" + path)
          GetJsonObject(newExpr, pathExpr)
        } else {
          newExpr
        }
    }
  }
}
