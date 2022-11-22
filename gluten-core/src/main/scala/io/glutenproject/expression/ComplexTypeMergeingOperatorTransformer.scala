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

package io.glutenproject.expression

import java.util.ArrayList
import scala.collection.JavaConverters

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.internal.Logging

import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}

class GreatestTransformer(children: Seq[Expression], original: Expression)
  extends Greatest(children: Seq[Expression])
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childrenNodes = new ArrayList[ExpressionNode]
    children.foreach(child => {
      val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
      if (!childNode.isInstanceOf[ExpressionNode]) {
        throw new UnsupportedOperationException(s"not supported yet.")
      }
      childrenNodes.add(childNode)
    })

    val childrenTypes = children.map(child => child.dataType)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.GREATEST, childrenTypes, FunctionConfig.OPT))
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, childrenNodes, typeNode)
  }
}


class LeastTransformer(children: Seq[Expression], original: Expression)
  extends Least(children: Seq[Expression])
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childrenNodes = new ArrayList[ExpressionNode]
    children.foreach(child => {
      val childNode = child.asInstanceOf[ExpressionTransformer].doTransform(args)
      if (!childNode.isInstanceOf[ExpressionNode]) {
        throw new UnsupportedOperationException(s"not supported yet.")
      }
      childrenNodes.add(childNode)
    })

    val childrenTypes = children.map(child => child.dataType)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.LEAST, childrenTypes, FunctionConfig.OPT))
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, childrenNodes, typeNode)
  }
}

object ComplexTypeMergeingOperatorTransformer {
  def createGreatestOrLeast(children: Seq[Expression], original: Expression)
    : Expression = original match {
    case g: Greatest =>
      new GreatestTransformer(children, original)
    case l: Least =>
      new LeastTransformer(children, original)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
