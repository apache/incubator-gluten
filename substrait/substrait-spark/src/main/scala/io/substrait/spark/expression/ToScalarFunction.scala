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
package io.substrait.spark.expression

import org.apache.spark.sql.catalyst.expressions.Expression

import io.substrait.`type`.Type
import io.substrait.expression.{Expression => SExpression, FunctionArg}
import io.substrait.extension.SimpleExtension

import scala.collection.JavaConverters

abstract class ToScalarFunction(functions: Seq[SimpleExtension.ScalarFunctionVariant])
  extends FunctionConverter[SimpleExtension.ScalarFunctionVariant, SExpression](functions) {

  override def generateBinding(
      sparkExp: Expression,
      function: SimpleExtension.ScalarFunctionVariant,
      arguments: Seq[FunctionArg],
      outputType: Type): SExpression = {
    SExpression.ScalarFunctionInvocation
      .builder()
      .outputType(outputType)
      .declaration(function)
      .addAllArguments(JavaConverters.asJavaIterable(arguments))
      .build()
  }

  def convert(expression: Expression, operands: Seq[SExpression]): Option[SExpression] = {
    Option(signatures.get(expression.getClass))
      .filter(m => m.allowedArgCount(2))
      .flatMap(m => m.attemptMatch(expression, operands))
  }
}

object ToScalarFunction {
  def apply(functions: Seq[SimpleExtension.ScalarFunctionVariant]): ToScalarFunction = {
    new ToScalarFunction(functions) {
      override def getSigs: Seq[Sig] = FunctionMappings.SCALAR_SIGS
    }
  }
}
