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
package io.glutenproject.utils

import io.glutenproject.expression.{AttributeReferenceTransformer, ConverterUtils, ExpressionTransformer}
import io.glutenproject.substrait.`type`.TypeNode
import io.glutenproject.substrait.SubstraitContext
import io.glutenproject.substrait.expression.ExpressionNode
import io.glutenproject.substrait.plan.{PlanBuilder, PlanNode}
import io.glutenproject.substrait.rel.{LocalFilesBuilder, RelBuilder}

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}

import com.google.common.collect.Lists

import java.util

object PlanNodesUtil {

  def genProjectionsPlanNode(keys: Seq[Expression], output: Seq[Attribute]): PlanNode = {
    val context = new SubstraitContext

    // input
    val iteratorIndex: Long = context.nextIteratorIndex
    var operatorId = context.nextOperatorId("ClickHouseBuildSideRelationReadIter")
    val inputIter = LocalFilesBuilder.makeLocalFiles(
      ConverterUtils.ITERATOR_PREFIX.concat(iteratorIndex.toString))
    context.setIteratorNode(iteratorIndex, inputIter)

    val typeList = new util.ArrayList[TypeNode]()
    val nameList = new util.ArrayList[String]()
    for (attr <- output) {
      typeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      nameList.add(ConverterUtils.genColumnNameWithExprId(attr))
    }
    val readRel =
      RelBuilder.makeReadRel(typeList, nameList, null, iteratorIndex, context, operatorId)

    val columnNames = keys.flatMap {
      case expression: AttributeReference =>
        Some(expression)
      case _ =>
        None
    }
    if (columnNames.isEmpty) {
      throw new IllegalArgumentException(s"Key column not found in expression: $keys")
    }
    if (columnNames.size != 1) {
      throw new IllegalArgumentException(s"Multiple key columns found in expression: $keys")
    }
    val columnExpr = columnNames.head

    val columnInOutput = output.zipWithIndex.filter {
      p: (Attribute, Int) => p._1.exprId == columnExpr.exprId || p._1.name == columnExpr.name
    }
    if (columnInOutput.isEmpty) {
      throw new IllegalStateException(
        s"Key $keys not found from build side relation output: $output")
    }
    if (columnInOutput.size != 1) {
      throw new IllegalStateException(
        s"More than one key $keys found from build side relation output: $output")
    }

    // project
    operatorId = context.nextOperatorId("ClickHouseBuildSideRelationProjection")
    val args = context.registeredFunction
    val columnarProjExpr: ExpressionTransformer =
      AttributeReferenceTransformer(
        columnExpr.name,
        columnInOutput.head._2,
        columnExpr.dataType,
        columnExpr.nullable,
        columnExpr.exprId,
        columnExpr.qualifier,
        columnExpr.metadata)

    val projExprNodeList = new java.util.ArrayList[ExpressionNode]()
    projExprNodeList.add(columnarProjExpr.doTransform(args))
    val projectNode =
      RelBuilder.makeProjectRel(readRel, projExprNodeList, context, operatorId, output.size)

    val outNames = new java.util.ArrayList[String]()
    for (k <- keys) {
      outNames.add(ConverterUtils.genColumnNameWithExprId(ConverterUtils.getAttrFromExpr(k)))
    }
    PlanBuilder.makePlan(context, Lists.newArrayList(projectNode), outNames)
  }
}
