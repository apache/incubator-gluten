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
import io.glutenproject.substrait.`type`.TypeBuilder
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._

class RegExpReplaceTransformer(first: Expression, second: Expression,
                               third: Expression,
                               forth: Expression, original: Expression)
  extends RegExpReplace(first: Expression, second: Expression,
    second: Expression, forth: Expression)
    with ExpressionTransformer
    with Logging {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val firstNode =
      first.asInstanceOf[ExpressionTransformer].doTransform(args)
    val secondNode =
      second.asInstanceOf[ExpressionTransformer].doTransform(args)
    val thirdNode =
      third.asInstanceOf[ExpressionTransformer].doTransform(args)
    val forthNode =
      forth.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!firstNode.isInstanceOf[ExpressionNode] ||
      !secondNode.isInstanceOf[ExpressionNode] ||
      !thirdNode.isInstanceOf[ExpressionNode] ||
      !forthNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"Not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(ConverterUtils.REGEXP_REPLACE,
      Seq(first.dataType, second.dataType, third.dataType, forth.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(firstNode, secondNode, thirdNode)
    val typeNode = TypeBuilder.makeString(original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

object QuaternaryExpressionTransformer {

  def create(first: Expression, second: Expression, third: Expression, forth: Expression,
             original: Expression): Expression = original match {
    case replace: RegExpReplace =>
      new RegExpReplaceTransformer(first, second, third, forth, replace)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
