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
package org.apache.spark.sql.extension

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

class CommonSubexpressionEliminateRule(session: SparkSession, conf: SQLConf)
  extends Rule[LogicalPlan]
  with Logging {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (plan.resolved) {
      visitPlan(plan)
    } else {
      plan
    }
  }

  private def visitPlan(plan: LogicalPlan): LogicalPlan = plan match {
    case project: Project => visitProject(project)
    case other =>
      val children = other.children.map(visitPlan)
      other.withNewChildren(children)
  }

  private def visitProject(project: Project): Project = {
    val input = project.child
    val equivalentExpressions: EquivalentExpressions = new EquivalentExpressions
    project.projectList.foreach(equivalentExpressions.addExprTree(_))

    // Get all the expressions that appear at least twice
    val commonExprs = equivalentExpressions.getCommonSubexpressions
    if (commonExprs.isEmpty) {
      return project
    }

    // Put the common expressions into a hash map
    var subExprEliminationExprs = Set.empty[ExpressionEquals]
    commonExprs.foreach {
      expr =>
        val exprEquals = new ExpressionEquals(expr)
        subExprEliminationExprs += exprEquals
    }

    // Replace the common expressions with the first expression that produces it.
    var newProjectList = project.projectList.map(_.transformDown {
      case expr: Expression =>
        val exprEquals = subExprEliminationExprs.find(_.equals(ExpressionEquals(expr)))
        if (exprEquals.isDefined) {
          Alias(exprEquals.get.e, expr.toString())()
        } else {
          expr
        }
      case other => other
    }).map(_.asInstanceOf[NamedExpression])

    newProjectList ++= subExprEliminationExprs.map(_.e.asInstanceOf[NamedExpression]).toSeq

    Project(newProjectList, input)
  }
}
