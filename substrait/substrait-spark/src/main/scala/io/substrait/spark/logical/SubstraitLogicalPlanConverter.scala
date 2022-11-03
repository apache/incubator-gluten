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

import io.substrait.spark.ExpressionConverter.EXTENSION_COLLECTION
import io.substrait.spark.expression._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.QueryExecution

import io.substrait.relation

import scala.collection.JavaConverters
import scala.collection.JavaConverters.asScalaBufferConverter

/**
 * RelVisitor to convert Substrait Rel plan to [[LogicalPlan]]. Unsupported Rel node will call
 * visitFallback and throw UnsupportedOperationException.
 */
class SubstraitLogicalPlanConverter(spark: SparkSession)
  extends relation.AbstractRelVisitor[LogicalPlan, RuntimeException] {

  private var currentOutput: Seq[UnresolvedAttribute] = Nil

  private val expressionConverter = new SubstraitExpressionConverter(
    BinaryExpressionConverter(JavaConverters.asScalaBuffer(EXTENSION_COLLECTION.scalarFunctions())))

  override def visitFallback(rel: relation.Rel): LogicalPlan =
    throw new UnsupportedOperationException(
      s"Rel $rel of type ${rel.getClass.getCanonicalName}" +
        s" not handled by visitor type ${getClass.getCanonicalName}.")

  override def visit(project: relation.Project): LogicalPlan = {
    val child = project.getInput.accept(this)
    val result = withCurrentOutput(currentOutput) {
      val projectList = project.getExpressions.asScala
        .map(expr => expr.accept(expressionConverter).asInstanceOf[NamedExpression])
      Project(projectList, child)
    }
    currentOutput = result.projectList.map(_.asInstanceOf[UnresolvedAttribute])
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

  private def withCurrentOutput[T](output: Seq[UnresolvedAttribute])(body: => T): T = {
    val oldOutput = expressionConverter.output
    try {
      expressionConverter.output = output
      body
    } finally {
      expressionConverter.output = oldOutput
    }
  }

  def convert(rel: relation.Rel): LogicalPlan = {
    val unresolvedRelation = rel.accept(this)
    val qe = new QueryExecution(spark, unresolvedRelation)
    qe.optimizedPlan
  }
}
