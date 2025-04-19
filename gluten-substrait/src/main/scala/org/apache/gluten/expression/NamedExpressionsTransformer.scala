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
package org.apache.gluten.expression

import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.expressions._

case class AliasTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression)
  extends UnaryExpressionTransformer {}

case class AttributeReferenceTransformer(
    substraitExprName: String,
    original: AttributeReference,
    bound: BoundReference)
  extends LeafExpressionTransformer {
  override def doTransform(context: SubstraitContext): ExpressionNode = {
    ExpressionBuilder.makeSelection(bound.ordinal.asInstanceOf[java.lang.Integer])
  }
}

case class BoundReferenceTransformer(substraitExprName: String, original: BoundReference)
  extends LeafExpressionTransformer {
  override def doTransform(context: SubstraitContext): ExpressionNode = {
    ExpressionBuilder.makeSelection(original.ordinal.asInstanceOf[java.lang.Integer])
  }
}
