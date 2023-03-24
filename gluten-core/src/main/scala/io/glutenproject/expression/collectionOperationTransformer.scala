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

import com.google.common.collect.Lists
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.spark.sql.catalyst.expressions.Expression

class BasicCollectionOperationTransfomer(substraitExprName: String,
                                     children: Seq[ExpressionTransformer],
                                 original: Expression) extends ExpressionTransformer {
  override def doTransform(args: Object): ExpressionNode = {
    val exprNodes = Lists.newArrayList[ExpressionNode]()
    children.foreach(expr => exprNodes.add(expr.doTransform(args)))
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      substraitExprName,
      Seq(original.dataType),
      FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, exprNodes, typeNode)
  }
}

class BinaryArgumentsCollectionOperationTransformer(substraitExprName: String,
                            left: ExpressionTransformer,
                            right: ExpressionTransformer,
                            original: Expression) extends ExpressionTransformer{
  override def doTransform(args: Object): ExpressionNode = {
    val children = Seq[ExpressionTransformer](left, right)
    new BasicCollectionOperationTransfomer(substraitExprName, children, original).doTransform(args)
  }
}

class UnaryArgumentCollectionOperationTransformer(substraitExprName: String,
                                                  child: ExpressionTransformer,
                                                  original: Expression) extends
  ExpressionTransformer {
  override def doTransform(args: Object): ExpressionNode = {
    new BasicCollectionOperationTransfomer(substraitExprName, Seq[ExpressionTransformer](child),
      original).doTransform(args)
  }
}
