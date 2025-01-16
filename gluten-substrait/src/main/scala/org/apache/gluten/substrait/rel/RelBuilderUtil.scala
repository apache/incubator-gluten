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
package org.apache.gluten.substrait.rel

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.expression.{ConverterUtils, ExpressionConverter}
import org.apache.gluten.substrait.`type`.TypeBuilder
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode
import org.apache.gluten.substrait.extensions.{AdvancedExtensionNode, ExtensionBuilder}

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}

import java.util.{List => JList}

import scala.collection.JavaConverters._

object RelBuilderUtil {

  def createProjectRel(
      inputAttributes: Seq[Attribute],
      input: RelNode,
      projExprNodeList: JList[ExpressionNode],
      context: SubstraitContext,
      operatorId: Long,
      validation: Boolean): RelNode = {
    val emitStartIndex = inputAttributes.size
    if (!validation) {
      RelBuilder.makeProjectRel(input, projExprNodeList, context, operatorId, emitStartIndex)
    } else {
      RelBuilder.makeProjectRel(
        input,
        projExprNodeList,
        createExtensionNode(inputAttributes),
        context,
        operatorId,
        emitStartIndex)
    }
  }

  def createFilterRel(
      context: SubstraitContext,
      condExpr: Expression,
      inputAttributes: Seq[Attribute],
      operatorId: Long,
      input: RelNode,
      validation: Boolean): RelNode = {
    assert(condExpr != null)
    val args = context.registeredFunction
    val condExprNode = ExpressionConverter
      .replaceWithExpressionTransformer(condExpr, inputAttributes)
      .doTransform(args)

    if (!validation) {
      RelBuilder.makeFilterRel(input, condExprNode, context, operatorId)
    } else {
      RelBuilder.makeFilterRel(
        input,
        condExprNode,
        createExtensionNode(inputAttributes),
        context,
        operatorId)
    }
  }

  def createExtensionNode(inputAttributes: Seq[Attribute]): AdvancedExtensionNode = {
    // Use an extension node to send the input types through Substrait plan for validation.
    val inputTypeNodeList = inputAttributes
      .map(attr => ConverterUtils.getTypeNode(attr.dataType, attr.nullable))
      .asJava
    ExtensionBuilder.makeAdvancedExtension(
      BackendsApiManager.getTransformerApiInstance.packPBMessage(
        TypeBuilder.makeStruct(false, inputTypeNodeList).toProtobuf))
  }
}
