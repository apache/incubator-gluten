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
package io.substrait.spark.logical

import io.substrait.spark.{DefaultRelVisitor, SparkExtension}
import io.substrait.spark.expression._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.substrait.ToSubstraitType

import io.substrait.`type`.{StringTypeVisitor, Type}
import io.substrait.{expression => exp}
import io.substrait.expression.{Expression => SExpression}
import io.substrait.relation

import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * RelVisitor to convert Substrait Rel plan to [[LogicalPlan]]. Unsupported Rel node will call
 * visitFallback and throw UnsupportedOperationException.
 */
class ToLogicalPlan(spark: SparkSession) extends DefaultRelVisitor[LogicalPlan] {

  private val expressionConverter =
    new ToSparkExpression(ToScalarFunction(SparkExtension.SparkScalarFunctions), Some(this))

  private def fromMeasure(measure: relation.Aggregate.Measure): AggregateExpression = {
    // this functions is called in createParentwithChild
    val arguments = measure.getFunction.arguments().asScala.zipWithIndex.map {
      case (arg, i) =>
        arg.accept(measure.getFunction.declaration(), i, expressionConverter)
    }.toSeq

    val aggregateFunction = SparkExtension.toAggregateFunction
      .getSparkExpressionFromSubstraitFunc(
        measure.getFunction.declaration.key,
        measure.getFunction.outputType)
      .map(sig => sig.makeCall(arguments))
      .map(_.asInstanceOf[AggregateFunction])
      .getOrElse({
        val msg = String.format(
          "Unable to convert Aggregate function %s(%s).",
          measure.getFunction.declaration.name,
          measure.getFunction.arguments.asScala
            .map {
              case ea: exp.EnumArg => ea.value.toString
              case e: SExpression => e.getType.accept(new StringTypeVisitor)
              case t: Type => t.accept(new StringTypeVisitor)
              case a => throw new IllegalStateException("Unexpected value: " + a)
            }
            .mkString(", ")
        )
        throw new IllegalArgumentException(msg)
      })
    AggregateExpression(
      aggregateFunction,
      ToAggregateFunction.toSpark(measure.getFunction.aggregationPhase()),
      ToAggregateFunction.toSpark(measure.getFunction.invocation()),
      None
    )
  }

  private def toNamedExpression(e: Expression): NamedExpression = e match {
    case ne: NamedExpression => ne
    case other => Alias(other, toPrettySQL(other))()
  }

  override def visit(aggregate: relation.Aggregate): LogicalPlan = {
    require(aggregate.getGroupings.size() == 1)
    val child = aggregate.getInput.accept(this)
    withChild(child) {
      val groupBy = aggregate.getGroupings
        .get(0)
        .getExpressions
        .asScala
        .map(expr => expr.accept(expressionConverter))

      val outputs = groupBy.map(toNamedExpression)
      val aggregateExpressions =
        aggregate.getMeasures.asScala.map(fromMeasure).map(toNamedExpression)
      Aggregate(groupBy.toSeq, (outputs ++= aggregateExpressions).toSeq, child)
    }
  }

  override def visit(join: relation.Join): LogicalPlan = {
    val left = join.getLeft.accept(this)
    val right = join.getRight.accept(this)
    withChild(left, right) {
      val condition = Option(join.getCondition.orElse(null))
        .map(_.accept(expressionConverter))

      val joinType = join.getJoinType match {
        case relation.Join.JoinType.INNER => Inner
        case relation.Join.JoinType.LEFT => LeftOuter
        case relation.Join.JoinType.RIGHT => RightOuter
        case relation.Join.JoinType.OUTER => FullOuter
        case relation.Join.JoinType.SEMI => LeftSemi
        case relation.Join.JoinType.ANTI => LeftAnti
        case relation.Join.JoinType.UNKNOWN =>
          throw new UnsupportedOperationException("Unknown join type is not supported")
      }
      Join(left, right, joinType, condition, hint = JoinHint.NONE)
    }
  }

  private def toSortOrder(sortField: SExpression.SortField): SortOrder = {
    val expression = sortField.expr().accept(expressionConverter)
    val (direction, nullOrdering) = sortField.direction() match {
      case SExpression.SortDirection.ASC_NULLS_FIRST => (Ascending, NullsFirst)
      case SExpression.SortDirection.DESC_NULLS_FIRST => (Descending, NullsFirst)
      case SExpression.SortDirection.ASC_NULLS_LAST => (Ascending, NullsLast)
      case SExpression.SortDirection.DESC_NULLS_LAST => (Descending, NullsLast)
      case other =>
        throw new RuntimeException(s"Unexpected Expression.SortDirection enum: $other !")
    }
    SortOrder(expression, direction, nullOrdering, Seq.empty)
  }
  override def visit(fetch: relation.Fetch): LogicalPlan = {
    val child = fetch.getInput.accept(this)
    val limit = Literal(fetch.getCount.getAsLong.intValue(), IntegerType)
    fetch.getOffset match {
      case 1L => GlobalLimit(limitExpr = limit, child = child)
      case -1L => LocalLimit(limitExpr = limit, child = child)
      case _ => visitFallback(fetch)
    }
  }
  override def visit(sort: relation.Sort): LogicalPlan = {
    val child = sort.getInput.accept(this)
    withChild(child) {
      val sortOrders = sort.getSortFields.asScala.map(toSortOrder).toSeq
      Sort(sortOrders, global = true, child)
    }
  }

  override def visit(project: relation.Project): LogicalPlan = {
    val child = project.getInput.accept(this)
    val (output, createProject) = child match {
      case a: Aggregate => (a.aggregateExpressions, false)
      case other => (other.output, true)
    }

    withOutput(output) {
      val projectList =
        project.getExpressions.asScala
          .map(expr => expr.accept(expressionConverter))
          .map(toNamedExpression).toSeq
      if (createProject) {
        Project(projectList, child)
      } else {
        val aggregate: Aggregate = child.asInstanceOf[Aggregate]
        aggregate.copy(aggregateExpressions = projectList)
      }
    }
  }

  override def visit(filter: relation.Filter): LogicalPlan = {
    val child = filter.getInput.accept(this)
    withChild(child) {
      val condition = filter.getCondition.accept(expressionConverter)
      Filter(condition, child)
    }
  }

  override def visit(emptyScan: relation.EmptyScan): LogicalPlan = {
    LocalRelation(ToSubstraitType.toAttribute(emptyScan.getInitialSchema))
  }
  override def visit(namedScan: relation.NamedScan): LogicalPlan = {
    resolve(UnresolvedRelation(namedScan.getNames.asScala.toSeq)) match {
      case m: MultiInstanceRelation => m.newInstance()
      case other => other
    }
  }

  private def withChild(child: LogicalPlan*)(body: => LogicalPlan): LogicalPlan = {
    val output = child.flatMap(_.output)
    withOutput(output)(body)
  }

  private def withOutput(output: Seq[NamedExpression])(body: => LogicalPlan): LogicalPlan = {
    expressionConverter.pushOutput(output)
    try {
      body
    } finally {
      expressionConverter.popOutput()
    }
  }
  private def resolve(plan: LogicalPlan): LogicalPlan = {
    val qe = new QueryExecution(spark, plan)
    qe.analyzed match {
      case SubqueryAlias(_, child) => child
      case other => other
    }
  }

  def convert(rel: relation.Rel): LogicalPlan = {
    val logicalPlan = rel.accept(this)
    require(logicalPlan.resolved)
    logicalPlan
  }
}
