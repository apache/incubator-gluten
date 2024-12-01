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
package org.apache.gluten.utils

import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode
import org.apache.gluten.substrait.plan.{PlanBuilder, PlanNode}
import org.apache.gluten.substrait.rel.RelBuilder

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

import com.google.common.collect.Lists

object PlanNodesUtil {

  def genProjectionsPlanNode(key: Expression, output: Seq[Attribute]): PlanNode = {
    val context = new SubstraitContext

    var operatorId = context.nextOperatorId("ClickHouseBuildSideRelationReadIter")
    val typeList = ConverterUtils.collectAttributeTypeNodes(output)
    val nameList = ConverterUtils.collectAttributeNamesWithExprId(output)
    val readRel = RelBuilder.makeReadRelForInputIterator(typeList, nameList, context, operatorId)

    // project
    operatorId = context.nextOperatorId("ClickHouseBuildSideRelationProjection")
    val args = context.registeredFunction

    val columnarProjExpr = ExpressionConverter
      .replaceWithExpressionTransformer(key, attributeSeq = output)

    val projExprNodeList = new java.util.ArrayList[ExpressionNode]()
    projExprNodeList.add(columnarProjExpr.doTransform(args))

    PlanBuilder.makePlan(
      context,
      Lists.newArrayList(
        RelBuilder.makeProjectRel(readRel, projExprNodeList, context, operatorId, output.size)),
      Lists.newArrayList(
        ConverterUtils.genColumnNameWithExprId(ConverterUtils.getAttrFromExpr(key)))
    )
  }
}
