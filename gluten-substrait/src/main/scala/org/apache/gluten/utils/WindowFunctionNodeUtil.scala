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

import org.apache.gluten.expression.ExpressionConverter
import org.apache.gluten.substrait.`type`.TypeNode
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode, WindowFunctionNode}

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.expressions.PreComputeRangeFrameBound

import java.util

import scala.collection.JavaConverters

object WindowFunctionNodeUtil {

  def makeWindowFunction(
      functionId: Integer,
      expressionNodes: util.List[ExpressionNode],
      columnName: String,
      outputTypeNode: TypeNode,
      upperBound: Expression,
      lowerBound: Expression,
      frameType: String,
      originalInputAttributes: util.List[Attribute]): WindowFunctionNode = {
    makeWindowFunction(
      functionId,
      expressionNodes,
      columnName,
      outputTypeNode,
      upperBound,
      lowerBound,
      frameType,
      false,
      originalInputAttributes)
  }

  def makeWindowFunction(
      functionId: Integer,
      expressionNodes: util.List[ExpressionNode],
      columnName: String,
      outputTypeNode: TypeNode,
      upperBound: Expression,
      lowerBound: Expression,
      frameType: String,
      ignoreNulls: Boolean,
      originalInputAttributes: util.List[Attribute]): WindowFunctionNode = {
    val upperBoundType = upperBound.sql
    val lowerBoundType = lowerBound.sql
    val upperBoundFoldable = upperBound.foldable
    val lowerBoundFoldable = lowerBound.foldable
    var upperBoundOffset = 0L
    var lowerBoundOffset = 0L
    var upperBoundRefNode: ExpressionNode = null
    var lowerBoundRefNode: ExpressionNode = null
    val isPreComputeRangeFrameUpperBound = upperBound.isInstanceOf[PreComputeRangeFrameBound]
    val isPreComputeRangeFrameLowerBound = lowerBound.isInstanceOf[PreComputeRangeFrameBound]
    if (isPreComputeRangeFrameUpperBound) {
      upperBoundOffset = upperBound.eval(null).toString.toLong
      upperBoundRefNode = ExpressionConverter
        .replaceWithExpressionTransformer(
          upperBound.asInstanceOf[PreComputeRangeFrameBound].child.toAttribute,
          JavaConverters.asScalaIteratorConverter(originalInputAttributes.iterator).asScala.toSeq
        )
        .doTransform(new util.HashMap[String, Long])
    }
    if (isPreComputeRangeFrameLowerBound) {
      lowerBoundOffset = lowerBound.eval(null).toString.toLong
      lowerBoundRefNode = ExpressionConverter
        .replaceWithExpressionTransformer(
          lowerBound.asInstanceOf[PreComputeRangeFrameBound].child.toAttribute,
          JavaConverters.asScalaIteratorConverter(originalInputAttributes.iterator).asScala.toSeq
        )
        .doTransform(new util.HashMap[String, Long])
    }
    ExpressionBuilder.makeWindowFunction(
      functionId,
      expressionNodes,
      columnName,
      outputTypeNode,
      upperBoundType,
      lowerBoundType,
      frameType,
      ignoreNulls,
      upperBoundFoldable,
      lowerBoundFoldable,
      upperBoundOffset,
      lowerBoundOffset,
      upperBoundRefNode,
      lowerBoundRefNode,
      isPreComputeRangeFrameUpperBound,
      isPreComputeRangeFrameLowerBound
    )
  }

}
