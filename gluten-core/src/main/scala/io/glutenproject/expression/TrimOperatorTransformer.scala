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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.internal.Logging
import io.glutenproject.substrait.expression._
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.expression.ConverterUtils.FunctionConfig


class StringTrimLeftTransformer(srcStr: Expression, original: Expression)
    extends StringTrimLeft(srcStr: Expression, None: Option[Expression])
    with ExpressionTransformer
    with Logging {

    override def doTransform(args: java.lang.Object): ExpressionNode = {
      val srcStrNode = srcStr.asInstanceOf[ExpressionTransformer].doTransform(args)
      if (!srcStrNode.isInstanceOf[ExpressionNode]) {
        throw new UnsupportedOperationException(s"not supported yet.")
      }

      val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
      val functionName =
        ConverterUtils.makeFuncName(ConverterUtils.LTRIM, Seq(srcStr.dataType), FunctionConfig.OPT)
      val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
      val expressNodes = Lists.newArrayList(srcStrNode.asInstanceOf[ExpressionNode])
      val typeNode = TypeBuilder.makeString(original.nullable)
      ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}


class StringTrimRightTransformer(srcStr: Expression, original: Expression)
    extends StringTrimRight(srcStr: Expression, None: Option[Expression])
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val srcStrNode = srcStr.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!srcStrNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName =
      ConverterUtils.makeFuncName(ConverterUtils.RTRIM, Seq(srcStr.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressNodes = Lists.newArrayList(srcStrNode.asInstanceOf[ExpressionNode])
    val typeNode = TypeBuilder.makeString(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

object TrimOperatorTransformer {
  def create(srcStr: Expression, original: Expression): Expression =
    original match {
      case l: StringTrimLeft =>
        new StringTrimLeftTransformer(srcStr, l)
      case r: StringTrimRight =>
        new StringTrimRightTransformer(srcStr, r)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
}

