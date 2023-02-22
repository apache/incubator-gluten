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

import java.util

import com.google.common.collect.Lists

import io.glutenproject.expression.{ConverterUtils, ExpressionConverter, ExpressionTransformer}
import io.glutenproject.substrait.`type`.TypeNode
import io.glutenproject.substrait.expression.ExpressionNode
import io.glutenproject.substrait.plan.{PlanBuilder, PlanNode}
import io.glutenproject.substrait.rel.{LocalFilesBuilder, RelBuilder}
import io.glutenproject.substrait.SubstraitContext

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

object PlanNodesUtil {

  def genProjectionsPlanNode(keys: Seq[Expression],
                             output: Seq[Attribute]): PlanNode = {
    val context = new SubstraitContext

    // input
    val iteratorIndex: Long = context.nextIteratorIndex
    var operatorId = context.nextOperatorId
    val inputIter = LocalFilesBuilder.makeLocalFiles(
      ConverterUtils.ITERATOR_PREFIX.concat(iteratorIndex.toString))
    context.setIteratorNode(iteratorIndex, inputIter)

    val typeList = new util.ArrayList[TypeNode]()
    val nameList = new util.ArrayList[String]()
    for (attr <- output) {
      typeList.add(ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      nameList.add(ConverterUtils.genColumnNameWithExprId(attr))
    }
    val readRel = RelBuilder.makeReadRel(
      typeList, nameList, null, iteratorIndex, context, operatorId)

    // project
    operatorId = context.nextOperatorId
    val args = context.registeredFunction
    val columnarProjExprs: Seq[ExpressionTransformer] = keys.map(expr => {
      ExpressionConverter
        .replaceWithExpressionTransformer(expr, attributeSeq = output)
    })
    val projExprNodeList = new java.util.ArrayList[ExpressionNode]()
    for (expr <- columnarProjExprs) {
      projExprNodeList.add(expr.doTransform(args))
    }
    val projectNode = RelBuilder.makeProjectRel(readRel, projExprNodeList, context, operatorId, output.size)

    val outNames = new java.util.ArrayList[String]()
    for (k <- keys) {
      outNames.add(ConverterUtils.genColumnNameWithExprId(ConverterUtils.getAttrFromExpr(k)))
    }
    PlanBuilder.makePlan(context, Lists.newArrayList(projectNode), outNames)
  }
}
