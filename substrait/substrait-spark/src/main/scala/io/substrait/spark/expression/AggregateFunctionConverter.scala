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
import org.apache.spark.sql.catalyst.expressions.aggregate._

import io.substrait.`type`.Type
import io.substrait.expression.{AggregateFunctionInvocation, Expression => PExp, ExpressionCreator, FunctionArg}
import io.substrait.function.SimpleExtension
import io.substrait.proto.AggregateFunction

import java.util.Collections

import scala.collection.JavaConverters

class AggregateFunctionConverter(functions: Seq[SimpleExtension.AggregateFunctionVariant])
  extends FunctionConverter[SimpleExtension.AggregateFunctionVariant, AggregateFunctionInvocation](
    functions) {

  override def generateBinding(
      sparkExp: Expression,
      function: SimpleExtension.AggregateFunctionVariant,
      arguments: Seq[FunctionArg],
      outputType: Type): AggregateFunctionInvocation = {

    val sparkAggregate = sparkExp.asInstanceOf[AggregateExpression]

    val invocation = if (sparkAggregate.isDistinct) {
      AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_DISTINCT
    } else {
      AggregateFunction.AggregationInvocation.AGGREGATION_INVOCATION_ALL
    }
    ExpressionCreator.aggregateFunction(
      function,
      outputType,
      to(sparkAggregate.mode),
      Collections.emptyList[PExp.SortField](),
      invocation,
      JavaConverters.asJavaIterable(arguments))
  }

  private def to(mode: AggregateMode): PExp.AggregationPhase = mode match {
    case Partial => PExp.AggregationPhase.INITIAL_TO_INTERMEDIATE
    case PartialMerge => PExp.AggregationPhase.INTERMEDIATE_TO_INTERMEDIATE
    case Final => PExp.AggregationPhase.INTERMEDIATE_TO_RESULT
    case other => throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
  override def getSigs: Seq[Sig] = FunctionMappings.AGGREGATE_SIGS

  def convert(
      expression: AggregateExpression,
      operands: Seq[PExp]): Option[AggregateFunctionInvocation] = {
    Option(signatures.get(expression.aggregateFunction.getClass))
      .filter(m => m.allowedArgCount(2))
      .flatMap(m => m.attemptMatch(expression, operands))
  }
}

object AggregateFunctionConverter {
  def apply(
      functions: Seq[SimpleExtension.AggregateFunctionVariant]): AggregateFunctionConverter = {
    new AggregateFunctionConverter(functions)
  }
}
