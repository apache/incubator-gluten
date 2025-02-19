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

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode

import org.apache.spark.sql.catalyst.expressions.LambdaFunction

case class LambdaFunctionTransformer(
    substraitExprName: String,
    function: ExpressionTransformer,
    arguments: Seq[ExpressionTransformer],
    original: LambdaFunction)
  extends ExpressionTransformer {
  override def children: Seq[ExpressionTransformer] = function +: arguments

  override def doTransform(context: SubstraitContext): ExpressionNode = {
    // Need to fallback when hidden be true as it's not supported in Velox
    if (original.hidden) {
      throw new GlutenNotSupportException(s"Unsupported LambdaFunction with hidden be true.")
    }
    super.doTransform(context)
  }
}
