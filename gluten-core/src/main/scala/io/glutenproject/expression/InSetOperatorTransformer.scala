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

import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._

class InSetTransformer(value: Expression, hset: Set[Any], original: Expression)
  extends InSet(value: Expression, hset: Set[Any])
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val leftNode = value.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!leftNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    InSetOperatorTransformer.toTransformer(value, leftNode, hset)
  }
}

object InSetOperatorTransformer {

  def create(value: Expression, hset: Set[Any], original: Expression): Expression = original match {
    case i: InSet =>
      new InSetTransformer(value, hset, i)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }

  def toTransformer(value: Expression,
                    leftNode: ExpressionNode,
                    values: Set[Any]): ExpressionNode = {
    val literalType = value.dataType
    val expressionNodes = new util.ArrayList[ExpressionNode](values.map({
      value =>
        ExpressionBuilder.makeLiteral(value, literalType, value == null)
    }).asJava)

    ExpressionBuilder.makeSingularOrListNode(leftNode, expressionNodes)
  }
}
