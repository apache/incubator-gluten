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

import io.substrait.spark.ExpressionConverter
import io.substrait.spark.ExpressionConverter.EXTENSION_COLLECTION
import io.substrait.spark.expression._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project, Subquery, SubqueryAlias}
import org.apache.spark.sql.execution.QueryExecution

import io.substrait.`type`.{StringTypeVisitor, Type}
import io.substrait.{expression => exp}
import io.substrait.expression.{Expression => SExpression}
import io.substrait.relation

import scala.collection.JavaConverters
import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * RelVisitor to convert Substrait Rel plan to [[LogicalPlan]]. Unsupported Rel node will call
 * visitFallback and throw UnsupportedOperationException.
 */
class SubstraitLogicalPlanConverter(spark: SparkSession)
  extends relation.AbstractRelVisitor[LogicalPlan, RuntimeException] {

  private var currentOutput: Seq[Attribute] = Nil

  private val expressionConverter = new SubstraitExpressionConverter(
    BinaryExpressionConverter(JavaConverters.asScalaBuffer(EXTENSION_COLLECTION.scalarFunctions())))

  override def visitFallback(rel: relation.Rel): LogicalPlan =
    throw new UnsupportedOperationException(
      s"Rel $rel of type ${rel.getClass.getCanonicalName}" +
        s" not handled by visitor type ${getClass.getCanonicalName}.")
  private def fromMeasure(measure: relation.Aggregate.Measure): AggregateExpression = {
    // this functions is called in withCurrentOutput
    val arguments = measure.getFunction.arguments().asScala.zipWithIndex.map {
      case (arg, i) =>
        arg.accept(measure.getFunction.declaration(), i, expressionConverter)
    }

    val aggregateFunction = ExpressionConverter.aggregateConverter
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
      AggregateFunctionConverter.toSpark(measure.getFunction.aggregationPhase()),
      AggregateFunctionConverter.toSpark(measure.getFunction.invocation()),
      None
    )
  }
  override def visit(aggregate: relation.Aggregate): LogicalPlan = {
    val child = aggregate.getInput.accept(this)
    require(aggregate.getGroupings.size() == 1)
    withCurrentOutput(currentOutput) {
      val groupBy = aggregate.getGroupings
        .get(0)
        .getExpressions
        .asScala
        .map(expr => expr.accept(expressionConverter))

      val outputs = groupBy.map {
        case e @ (_: NamedExpression) => e
        case other => UnresolvedAlias(other)
      }
      val aggregateExpressions =
        aggregate.getMeasures.asScala.map(fromMeasure).map(e => UnresolvedAlias(e))
      Aggregate(groupBy, outputs ++= aggregateExpressions, child)
    }
  }
  override def visit(project: relation.Project): LogicalPlan = {

    /**
     * Always resolve [[Project]] to avoid `Project(Add(c, d) , _)`, in such case we can't create a
     * [[UnresolvedAttribute]]
     */
    val child = resolve(project.getInput.accept(this))
    val projectList = withCurrentOutput(currentOutput) {
      project.getExpressions.asScala.map(expr => expr.accept(expressionConverter)).map {
        case e @ (_: NamedExpression) => e
        case other => Alias(other, "project")()
      }
    }
    val result = Project(projectList, child)
    currentOutput = result.output
    result
  }

  override def visit(filter: relation.Filter): LogicalPlan = {
    val child = filter.getInput.accept(this)
    withCurrentOutput(currentOutput) {
      val condition = filter.getCondition.accept(expressionConverter)
      Filter(condition, child)
    }
  }

  override def visit(namedScan: relation.NamedScan): LogicalPlan = {
    currentOutput = namedScan.getInitialSchema.names().asScala.map(s => UnresolvedAttribute(s))
    UnresolvedRelation(namedScan.getNames.asScala)
  }

  private def withCurrentOutput[T](output: Seq[Attribute])(body: => T): T = {
    val oldOutput = expressionConverter.output
    try {
      expressionConverter.output = output
      body
    } finally {
      expressionConverter.output = oldOutput
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
    val unresolvedRelation = rel.accept(this)
    val qe = new QueryExecution(spark, unresolvedRelation)
    qe.optimizedPlan
  }
}
