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
package io.glutenproject.extension

import io.glutenproject.GlutenConfig
import io.glutenproject.extension.CommonSubexpressionEliminateRule._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

class CommonSubexpressionEliminateRule(session: SparkSession, conf: SQLConf)
  extends Rule[LogicalPlan]
  with Logging {

  private var lastPlan: LogicalPlan = null

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val newPlan =
      if (
        plan.resolved && GlutenConfig.getConf.enableGluten
        && GlutenConfig.getConf.enableCommonSubexpressionEliminate && !plan.fastEquals(lastPlan)
      ) {
        logTrace(s"visit root plan:\n$plan")
        lastPlan = plan
        visitPlan(plan)
      } else {
        plan
      }
    newPlan
  }

  private case class AliasAndAttribute(alias: Alias, attribute: Attribute)

  private case class RewriteContext(exprs: Seq[Expression], child: LogicalPlan)

  private def visitPlan(plan: LogicalPlan): LogicalPlan = {
    // Don't apply CSE for Aggregate, see https://github.com/oap-project/gluten/issues/4642
    val newPlan = plan match {
      case project: Project => visitProject(project)
      // TODO: CSE in Filter doesn't work for unknown reason, need to fix it later
      // case filter: Filter => visitFilter(filter)
      case window: Window => visitWindow(window)
      case sort: Sort => visitSort(sort)
      case other =>
        val children = other.children.map(visitPlan)
        other.withNewChildren(children)
    }

    if (newPlan.output.size == plan.output.size) {
      return newPlan
    }

    // Add a Project to trim unnecessary attributes(which are always at the end of the output)
    val postProjectList = newPlan.output.take(plan.output.size)
    Project(postProjectList, newPlan)
  }

  private def replaceCommonExprWithAttribute(
      expr: Expression,
      commonExprMap: mutable.HashMap[ExpressionEquals, AliasAndAttribute]): Expression = {
    val exprEquals = commonExprMap.get(ExpressionEquals(expr))
    if (exprEquals.isDefined) {
      exprEquals.get.attribute
    } else {
      expr.mapChildren(replaceCommonExprWithAttribute(_, commonExprMap))
    }
  }

  private def isValidCommonExpr(expr: Expression): Boolean = {
    if (
      (expr.isInstanceOf[Unevaluable] && !expr.isInstanceOf[AttributeReference])
      || expr.isInstanceOf[AggregateFunction]
      || (expr.isInstanceOf[AttributeReference]
        && expr.asInstanceOf[AttributeReference].name == VirtualColumn.groupingIdName)
    ) {
      logTrace(s"check common expression failed $expr, class ${expr.getClass.toString}")
      return false
    }

    expr.children.forall(isValidCommonExpr)
  }

  private def rewrite(inputCtx: RewriteContext, planName: String): RewriteContext = {
    val equivalentExpressions = new EquivalentExpressions
    inputCtx.exprs.foreach(equivalentExpressions.addExprTree(_))

    // Get all the expressions that appear at least twice
    val newChild = visitPlan(inputCtx.child)
    logTrace(
      s"start rewrite $planName\ninput exprs:\n${inputCtx.exprs}\ninput child:\n${inputCtx.child}")

    val commonExprs = equivalentExpressions.getCommonSubexpressions
    // Put the common expressions into a hash map
    val commonExprMap = mutable.HashMap.empty[ExpressionEquals, AliasAndAttribute]
    commonExprs.foreach {
      expr =>
        if (!expr.foldable && !expr.isInstanceOf[Attribute] && isValidCommonExpr(expr)) {
          logTrace(s"common subexpression $expr, class ${expr.getClass.toString}")
          val exprEquals = ExpressionEquals(expr)
          val alias = Alias(expr, s"cse_alias_${exprCounter.getAndIncrement()}")()
          commonExprMap.put(exprEquals, AliasAndAttribute(alias, alias.toAttribute))
        }
    }

    if (commonExprMap.isEmpty) {
      logTrace(s"commonExprMap is empty, all exprs: ${equivalentExpressions.debugString(true)}")
      return RewriteContext(inputCtx.exprs, newChild)
    }

    // Generate pre-project as new child
    val preProjectList = newChild.output ++ commonExprMap.values.map(_.alias)
    val preProject = Project(preProjectList, newChild)
    logTrace(s"newChild after rewrite:\n$preProject")

    // Replace the common expressions with the first expression that produces it.
    try {
      val newExprs = inputCtx.exprs
        .map(replaceCommonExprWithAttribute(_, commonExprMap))
      logTrace(s"newExprs after rewrite: $newExprs")
      RewriteContext(newExprs, preProject)
    } catch {
      case e: Exception =>
        logWarning(
          s"Common subexpression eliminate failed with exception: ${e.getMessage}" +
            s" while replace ${inputCtx.exprs} with $commonExprMap, fallback now")
        RewriteContext(inputCtx.exprs, newChild)
    }
  }

  private def visitProject(project: Project): Project = {
    val inputCtx = RewriteContext(project.projectList, project.child)
    val outputCtx = rewrite(inputCtx, project.getClass.getSimpleName)
    Project(outputCtx.exprs.map(_.asInstanceOf[NamedExpression]), outputCtx.child)
  }

  private def visitFilter(filter: Filter): Filter = {
    val inputCtx = RewriteContext(Seq(filter.condition), filter.child)
    val outputCtx = rewrite(inputCtx, filter.getClass.getSimpleName)
    Filter(outputCtx.exprs.head, outputCtx.child)
  }

  private def visitWindow(window: Window): Window = {
    val inputCtx = RewriteContext(window.windowExpressions, window.child)
    val outputCtx = rewrite(inputCtx, window.getClass.getSimpleName)
    Window(
      outputCtx.exprs.map(_.asInstanceOf[NamedExpression]),
      window.partitionSpec,
      window.orderSpec,
      outputCtx.child)
  }

  private def visitSort(sort: Sort): Sort = {
    val exprs = sort.order.flatMap(_.children)
    val inputCtx = RewriteContext(exprs, sort.child)
    val outputCtx = rewrite(inputCtx, sort.getClass.getSimpleName)

    var start = 0
    var newOrder = Seq.empty[SortOrder]
    sort.order.foreach(
      order => {
        val childrenSize = order.children.size
        val newChildren = outputCtx.exprs.slice(start, start + childrenSize)
        newOrder = newOrder :+ order.withNewChildren(newChildren).asInstanceOf[SortOrder]
        start += childrenSize
      })

    Sort(newOrder, sort.global, outputCtx.child)
  }
}

object CommonSubexpressionEliminateRule {
  private val exprCounter = new AtomicInteger(0)
}
