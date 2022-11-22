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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.internal.Logging
import io.glutenproject.substrait.expression._
import io.glutenproject.substrait.`type`._
import java.util.ArrayList

class EulerNumberTransformer(original: Expression)
  extends EulerNumber
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.E, Seq()))
    val typeNode = TypeBuilder.makeFP64(original.nullable)
    val expressionNodes = new java.util.ArrayList[ExpressionNode]
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class PiTransformer(original: Expression)
  extends Pi
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(ConverterUtils.PI, Seq()))
    val typeNode = TypeBuilder.makeFP64(original.nullable)
    val expressionNodes = new java.util.ArrayList[ExpressionNode]
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}


object LeafOperatorTransformer {
  def create(original: Expression): Expression = original match {
    case EulerNumber() =>
      new EulerNumberTransformer(original)
    case Pi() =>
      new PiTransformer(original)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
