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
package io.substrait.workaround

import io.substrait.`type`.Type
import io.substrait.expression.{Expression, ExpressionVisitor}

case class SubstraitTypeExpression(t: Type) extends Expression {
  override def getType: Type = t
  override def accept[R, E <: Throwable](visitor: ExpressionVisitor[R, E]): R =
    throw new UnsupportedOperationException("TypeHolderExpression")
}
