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

import java.util

import scala.collection.JavaConverters._
import com.google.common.collect.Lists
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, StringType}

class InTransformer(value: Expression, list: Seq[Expression], original: Expression)
    extends In(value: Expression, list: Seq[Expression])
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val leftNode = value.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!leftNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    val expressionNodes = Lists.newArrayList(leftNode.asInstanceOf[ExpressionNode])

    // Stores the values in a List Literal.
    val values : Seq[Any] = list.map(value => {
      value.asInstanceOf[Literal].value
    })
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

    val typeNode = TypeBuilder.makeBoolean(false)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

object InExpressionTransformer {

  def create(value: Expression, list: Seq[Expression], original: Expression): Expression =
    original match {
      case i: In =>
        new InTransformer(value, list, i)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
