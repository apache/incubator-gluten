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

import scala.collection.JavaConverters._

import com.google.common.collect.Lists
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import java.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

class InSetTransformer(value: Expression, hset: Set[Any], original: Expression)
  extends InSet(value: Expression, hset: Set[Any])
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val leftNode = value.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!leftNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    InSetOperatorTransformer.toTransformer(args, value, leftNode, hset, original.nullable)
  }
}

object InSetOperatorTransformer {

  def create(value: Expression, hset: Set[Any], original: Expression): Expression = original match {
    case i: InSet =>
      new InSetTransformer(value, hset, i)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }

  def toTransformer(args: java.lang.Object,
                    value: Expression,
                    leftNode: ExpressionNode,
                    values: Set[Any],
                    outputNullable: Boolean): ExpressionNode = {
    val expressionNodes = Lists.newArrayList(leftNode.asInstanceOf[ExpressionNode])
    val listNode = value.dataType match {
      case _: IntegerType =>
        val valueList = new util.ArrayList[java.lang.Integer](values.map(value =>
          value.asInstanceOf[java.lang.Integer]).asJava)
        ExpressionBuilder.makeIntList(valueList)
      case _: LongType =>
        val valueList = new util.ArrayList[java.lang.Long](values.map(value =>
          value.asInstanceOf[java.lang.Long]).asJava)
        ExpressionBuilder.makeLongList(valueList)
      case _: DoubleType =>
        val valueList = new util.ArrayList[java.lang.Double](values.map(value =>
          value.asInstanceOf[java.lang.Double]).asJava)
        ExpressionBuilder.makeDoubleList(valueList)
      case _: DateType =>
        val valueList = new util.ArrayList[java.lang.Integer](values.map(value =>
          value.asInstanceOf[java.lang.Integer]).asJava)
        ExpressionBuilder.makeDateList(valueList)
      case _: StringType =>
        val valueList = new util.ArrayList[java.lang.String](values.map(value =>
          value.toString).asJava)
        ExpressionBuilder.makeStringList(valueList)
      case other =>
        throw new UnsupportedOperationException(s"$other is not supported.")
    }
    expressionNodes.add(listNode)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      ConverterUtils.IN, Seq(value.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)

    val typeNode = TypeBuilder.makeBoolean(outputNullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}
