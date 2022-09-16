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

package io.glutenproject.execution

import com.google.common.collect.Lists

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.{ConverterUtils, ExpressionConverter, ExpressionTransformer}
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.`type`.ColumnTypeNode
import io.glutenproject.substrait.plan.PlanBuilder
import io.glutenproject.substrait.rel.RelBuilder
import io.glutenproject.vectorized.ExpressionEvaluator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.InSubqueryExec
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

trait BasicScanExecTransformer extends TransformSupport {

  def filterExprs(): Seq[Expression]

  def outputAttributes(): Seq[Attribute]

  def getPartitions: Seq[Seq[InputPartition]]

  def getFlattenPartitions: Seq[InputPartition]

  def getPartitionSchemas: StructType

  def doExecuteColumnarInternal(): RDD[ColumnarBatch] = {
    val numOutputRows = longMetric("outputRows")
    val numOutputVectors = longMetric("outputVectors")
    val scanTime = longMetric("scanTime")
    val substraitContext = new SubstraitContext
    val transformContext = doTransform(substraitContext)
    val outNames = new java.util.ArrayList[String]()
    for (attr <- outputAttributes()) {
      outNames.add(ConverterUtils.genColumnNameWithExprId(attr))
    }
    val planNode =
      PlanBuilder.makePlan(substraitContext, Lists.newArrayList(transformContext.root), outNames)
    val fileFormat = ConverterUtils.getFileFormat(this)

    BackendsApiManager.getIteratorApiInstance.genNativeFileScanRDD(
      sparkContext,
      WholestageTransformContext(outputAttributes(),
        outputAttributes(),
        planNode,
        substraitContext),
      fileFormat,
      getFlattenPartitions,
      numOutputRows,
      numOutputVectors,
      scanTime
    )
  }

  override def doValidate(): Boolean = {
    val substraitContext = new SubstraitContext
    val relNode =
      try {
        doTransform(substraitContext).root
      } catch {
        case e: Throwable =>
          logDebug(s"Validation failed for ${this.getClass.toString} due to ${e.getMessage}")
          return false
      }

    if (GlutenConfig.getConf.enableNativeValidation) {
      val validator = new ExpressionEvaluator()
      val planNode = PlanBuilder.makePlan(substraitContext, Lists.newArrayList(relNode))
      validator.doValidate(planNode.toProtobuf.toByteArray)
    } else {
      true
    }
  }

  override def doTransform(context: SubstraitContext): TransformContext = {
    val output = outputAttributes()
    val typeNodes = ConverterUtils.getTypeNodeFromAttributes(output)
    val partitionSchemas = getPartitionSchemas
    val nameList = new java.util.ArrayList[String]()
    val columnTypeNodes = new java.util.ArrayList[ColumnTypeNode]()
    for (attr <- output) {
      nameList.add(attr.name)
      if (partitionSchemas.exists(_.name.equals(attr.name))) {
        columnTypeNodes.add(new ColumnTypeNode(1))
      } else {
        columnTypeNodes.add(new ColumnTypeNode(0))
      }
    }
    // Will put all filter expressions into an AND expression
    val transformer = filterExprs()
      .reduceLeftOption(And)
      .map(ExpressionConverter.replaceWithExpressionTransformer(_, output))
    val filterNodes = transformer.map(
      _.asInstanceOf[ExpressionTransformer].doTransform(context.registeredFunction))
    val exprNode = filterNodes.orNull

    val relNode = RelBuilder.makeReadRel(
      typeNodes, nameList, columnTypeNodes, exprNode, context, context.nextOperatorId)
    TransformContext(output, output, relNode)
  }

  def executeInSubqueryForDynamicPruningExpression(inSubquery: InSubqueryExec): Unit = {
    if (!inSubquery.values().isDefined) inSubquery.updateResult()
  }
}
