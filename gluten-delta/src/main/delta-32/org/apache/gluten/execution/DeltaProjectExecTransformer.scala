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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter, ExpressionTransformer}
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.extensions.ExtensionBuilder
import org.apache.gluten.substrait.rel.{RelBuilder, RelNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CaseWhen, Expression, NamedExpression, NullIntolerant, PredicateHelper, SortOrder}
import org.apache.spark.sql.delta.metric.IncrementMetric
import org.apache.spark.sql.execution.{ExplainUtils, OrderPreservingNodeShim, PartitioningPreservingNodeShim, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetric

import scala.collection.JavaConverters._
import scala.collection.mutable

case class DeltaProjectExecTransformer private (projectList: Seq[NamedExpression], child: SparkPlan)
  extends UnaryTransformSupport
  with OrderPreservingNodeShim
  with PartitioningPreservingNodeShim
  with PredicateHelper
  with Logging {

  private var extraMetrics = mutable.Seq.empty[(String, SQLMetric)]

  // Note: "metrics" is made transient to avoid sending driver-side metrics to tasks.
  @transient override lazy val metrics =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetrics(sparkContext)

  override protected def doValidateInternal(): ValidationResult = {
    val substraitContext = new SubstraitContext
    // Firstly, need to check if the Substrait plan for this operator can be successfully generated.
    val operatorId = substraitContext.nextOperatorId(this.nodeName)
    val relNode =
      getRelNode(substraitContext, projectList, child.output, operatorId, null, validation = true)
    // Then, validate the generated plan in native engine.
    doNativeValidation(substraitContext, relNode)
  }

  override def isNullIntolerant(expr: Expression): Boolean = expr match {
    case e: NullIntolerant => e.children.forall(isNullIntolerant)
    case _ => false
  }

  override def metricsUpdater(): MetricsUpdater =
    BackendsApiManager.getMetricsApiInstance.genProjectTransformerMetricsUpdater(
      metrics,
      extraMetrics)

  override def doTransform(context: SubstraitContext): TransformContext = {
    val childCtx = child.asInstanceOf[TransformSupport].transform(context)
    val operatorId = context.nextOperatorId(this.nodeName)
    if ((projectList == null || projectList.isEmpty) && childCtx != null) {
      // The computing for this project is not needed.
      // the child may be an input adapter and childCtx is null. In this case we want to
      // make a read node with non-empty base_schema.
      context.registerEmptyRelToOperator(operatorId)
      return childCtx
    }

    val currRel =
      getRelNode(context, projectList, child.output, operatorId, childCtx.root, validation = false)
    assert(currRel != null, "Project Rel should be valid")
    TransformContext(childCtx.outputAttributes, output, currRel)
  }

  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering

  override protected def outputExpressions: Seq[NamedExpression] = projectList

  def getRelNode(
      context: SubstraitContext,
      projectList: Seq[NamedExpression],
      originalInputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    val args = context.registeredFunction
    val newProjectList = genNewProjectList(projectList)
    val columnarProjExprs: Seq[ExpressionTransformer] = ExpressionConverter
      .replaceWithExpressionTransformer(newProjectList, attributeSeq = originalInputAttributes)
    val projExprNodeList = columnarProjExprs.map(_.doTransform(args)).asJava
    val emitStartIndex = originalInputAttributes.size
    if (!validation) {
      RelBuilder.makeProjectRel(input, projExprNodeList, context, operatorId, emitStartIndex)
    } else {
      // Use a extension node to send the input types through Substrait plan for validation.
      val inputTypeNodeList = originalInputAttributes
        .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
        .asJava
      val extensionNode = ExtensionBuilder.makeAdvancedExtension(
        BackendsApiManager.getTransformerApiInstance.packPBMessage(
          TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
      RelBuilder.makeProjectRel(
        input,
        projExprNodeList,
        extensionNode,
        context,
        operatorId,
        emitStartIndex)
    }
  }

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", projectList)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |""".stripMargin
  }

  override protected def withNewChildInternal(newChild: SparkPlan): DeltaProjectExecTransformer =
    copy(child = newChild)

  def genNewProjectList(projectList: Seq[NamedExpression]): Seq[NamedExpression] = {
    projectList.map {
      case alias: Alias =>
        alias.child match {
          case IncrementMetric(child, metric) =>
            extraMetrics :+= (alias.child.prettyName, metric)
            Alias(child = child, name = alias.name)()

          case CaseWhen(branches, elseValue) =>
            val newBranches = branches.map {
              case (expr1, expr2: IncrementMetric) =>
                extraMetrics :+= (expr2.prettyName, expr2.metric)
                (expr1, expr2.child)
              case other => other
            }

            val newElseValue = elseValue match {
              case Some(IncrementMetric(child: IncrementMetric, metric)) =>
                extraMetrics :+= (child.prettyName, metric)
                extraMetrics :+= (child.prettyName, child.metric)
                Some(child.child)
              case _ => elseValue
            }

            Alias(
              child = CaseWhen(newBranches, newElseValue),
              name = alias.name
            )()

          case _ =>
            alias
        }
      case other => other
    }
  }
}
