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

import scala.collection.mutable.ArrayBuffer

import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DataType

class Murmur3HashTransformer(exps: Seq[Expression], original: Expression)
  extends Murmur3Hash(exps: Seq[Expression])
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val nodes = new java.util.ArrayList[ExpressionNode]()
    val arrayBuffer = new ArrayBuffer[DataType]()
    exps.foreach(expression => {
      val expressionNode = expression.asInstanceOf[ExpressionTransformer].doTransform(args)
      if (!expressionNode.isInstanceOf[ExpressionNode]) {
        throw new UnsupportedOperationException(s"Not supported yet.")
      }
      arrayBuffer.append(expression.dataType)
      nodes.add(expressionNode)
    })
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(ConverterUtils.MURMUR3HASH,
      arrayBuffer, FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, nodes, typeNode)
  }
}
