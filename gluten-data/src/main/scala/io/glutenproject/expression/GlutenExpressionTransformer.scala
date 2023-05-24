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
import io.glutenproject.expression.ExpressionConverter.replaceWithExpressionTransformer
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{IntegerType, LongType}

class GlutenAliasTransformer(substraitExprName: String,
                             child: ExpressionTransformer,
                             original: Expression)
  extends AliasBaseTransformer(substraitExprName, child, original) {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    child.doTransform(args)
  }
}

class GlutenNamedStructTransformer(substraitExprName: String,
                                   original: CreateNamedStruct,
                                   attributeSeq: Seq[Attribute]) extends ExpressionTransformer {
  override def doTransform(args: Object): ExpressionNode = {
    var childrenTransformers = Seq[ExpressionTransformer]()
    original.valExprs.foreach(child =>
      childrenTransformers = childrenTransformers :+
        replaceWithExpressionTransformer(child, attributeSeq)
    )
    val expressionNodes = Lists.newArrayList[ExpressionNode]()
    for (elem <- childrenTransformers) {
      expressionNodes.add(elem.doTransform(args))
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils
      .makeFuncName(substraitExprName, Seq(original.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}


case class GlutenHashExpressionTransformer(substraitExprName: String,
                                           exps: Seq[ExpressionTransformer],
                                           original: Expression)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // As of Spark 3.3, there are 3 kinds of HashExpression.
    // HiveHash is not supported in native backend and will fail native validation.
    val (seedNode, seedType) = original match {
      case XxHash64(_, seed) =>
        (ExpressionBuilder.makeLongLiteral(seed), LongType)
      case Murmur3Hash(_, seed) =>
        (ExpressionBuilder.makeIntLiteral(seed), IntegerType)
      case HiveHash(_) =>
        (ExpressionBuilder.makeIntLiteral(0), IntegerType)
    }
    val nodes = new java.util.ArrayList[ExpressionNode]()
    // Seed as the first argument
    nodes.add(seedNode)
    exps.foreach(expression => {
      nodes.add(expression.doTransform(args))
    })
    val childrenTypes = seedType +: original.children.map(child => child.dataType)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(substraitExprName,
      childrenTypes, FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, nodes, typeNode)
  }
}
