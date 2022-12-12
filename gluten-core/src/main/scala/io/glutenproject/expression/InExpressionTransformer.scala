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

import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DataType

import scala.collection.JavaConverters._

class InTransformer(value: ExpressionTransformer,
                    list: Seq[Expression],
                    valueType: DataType,
                    original: Expression)
  extends ExpressionTransformer with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // Stores the values in a List Literal.
    val values: Set[Any] = list.map(v => {
      v.asInstanceOf[Literal].value
    }).toSet

    InExpressionTransformer.toTransformer(value, value.doTransform(args), values, valueType)
  }
}

class InSetTransformer(value: ExpressionTransformer,
                       hset: Set[Any],
                       valueType: DataType,
                       original: Expression)
  extends ExpressionTransformer with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    InExpressionTransformer.toTransformer(value, value.doTransform(args), hset, valueType)
  }
}

object InExpressionTransformer {

  def toTransformer(value: ExpressionTransformer,
                    leftNode: ExpressionNode,
                    values: Set[Any],
                    valueType: DataType): ExpressionNode = {
    val expressionNodes = new java.util.ArrayList[ExpressionNode](values.map({
      value =>
        ExpressionBuilder.makeLiteral(value, valueType, value == null)
    }).asJava)

    ExpressionBuilder.makeSingularOrListNode(leftNode, expressionNodes)
  }
}
