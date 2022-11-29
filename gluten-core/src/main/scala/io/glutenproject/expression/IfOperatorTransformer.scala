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

import io.glutenproject.substrait.expression.ExpressionNode
import io.glutenproject.substrait.expression.IfThenNode

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import java.util.ArrayList

class IfTransformer(predicate: Expression, trueValue: Expression,
                    falseValue: Expression, original: Expression)
  extends If(predicate: Expression, trueValue: Expression, falseValue: Expression)
    with ExpressionTransformer
    with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    if (!predicate.isInstanceOf[ExpressionTransformer] ||
      !trueValue.isInstanceOf[ExpressionTransformer] ||
      !falseValue.isInstanceOf[ExpressionTransformer]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    val ifNodes: ArrayList[ExpressionNode] = new ArrayList[ExpressionNode]
    ifNodes.add(predicate.asInstanceOf[ExpressionTransformer].doTransform(args))

    val thenNodes: ArrayList[ExpressionNode] = new ArrayList[ExpressionNode]
    thenNodes.add(trueValue.asInstanceOf[ExpressionTransformer].doTransform(args))

    val elseValueNode = falseValue.asInstanceOf[ExpressionTransformer].doTransform(args)
    new IfThenNode(ifNodes, thenNodes, elseValueNode)
  }
}

object IfOperatorTransformer {

  def create(predicate: Expression, trueValue: Expression,
             falseValue: Expression, original: Expression): Expression = original match {
    case i: If =>
      new IfTransformer(predicate, trueValue, falseValue, original)
    case other =>
      throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
}
