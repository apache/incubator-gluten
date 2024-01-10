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
import io.substrait.expression.{AggregateFunctionInvocation, Expression => SExpression, ExpressionCreator, FunctionArg}
import io.substrait.extension.SimpleExtension

import java.util.Collections

import scala.collection.JavaConverters

abstract class ToAggregateFunction(functions: Seq[SimpleExtension.AggregateFunctionVariant])
  extends FunctionConverter[SimpleExtension.AggregateFunctionVariant, AggregateFunctionInvocation](
    functions) {

  override def generateBinding(
      sparkExp: Expression,
      function: SimpleExtension.AggregateFunctionVariant,
      arguments: Seq[FunctionArg],
      outputType: Type): AggregateFunctionInvocation = {

    val sparkAggregate = sparkExp.asInstanceOf[AggregateExpression]

    ExpressionCreator.aggregateFunction(
      function,
      outputType,
      ToAggregateFunction.fromSpark(sparkAggregate.mode),
      Collections.emptyList[SExpression.SortField](),
      ToAggregateFunction.fromSpark(sparkAggregate.isDistinct),
      JavaConverters.asJavaIterable(arguments)
    )
  }

  def convert(
      expression: AggregateExpression,
      operands: Seq[SExpression]): Option[AggregateFunctionInvocation] = {
    Option(signatures.get(expression.aggregateFunction.getClass))
      .filter(m => m.allowedArgCount(2))
      .flatMap(m => m.attemptMatch(expression, operands))
  }

  def apply(
      expression: AggregateExpression,
      operands: Seq[SExpression]): AggregateFunctionInvocation = {
    convert(expression, operands).getOrElse(
      throw new UnsupportedOperationException(
        s"Unable to find binding for call ${expression.aggregateFunction}"))
  }
}

object ToAggregateFunction {
  def fromSpark(mode: AggregateMode): SExpression.AggregationPhase = mode match {
    case Partial => SExpression.AggregationPhase.INITIAL_TO_INTERMEDIATE
    case PartialMerge => SExpression.AggregationPhase.INTERMEDIATE_TO_INTERMEDIATE
    case Final => SExpression.AggregationPhase.INTERMEDIATE_TO_RESULT
    case Complete => SExpression.AggregationPhase.INITIAL_TO_RESULT
    case other => throw new UnsupportedOperationException(s"not currently supported: $other.")
  }
  def toSpark(phase: SExpression.AggregationPhase): AggregateMode = phase match {
    case SExpression.AggregationPhase.INITIAL_TO_INTERMEDIATE => Partial
    case SExpression.AggregationPhase.INTERMEDIATE_TO_INTERMEDIATE => PartialMerge
    case SExpression.AggregationPhase.INTERMEDIATE_TO_RESULT => Final
    case SExpression.AggregationPhase.INITIAL_TO_RESULT => Complete
  }
  def fromSpark(isDistinct: Boolean): SExpression.AggregationInvocation = if (isDistinct) {
    SExpression.AggregationInvocation.DISTINCT
  } else {
    SExpression.AggregationInvocation.ALL
  }

  def toSpark(innovation: SExpression.AggregationInvocation): Boolean = innovation match {
    case SExpression.AggregationInvocation.DISTINCT => true
    case _ => false
  }

  def apply(functions: Seq[SimpleExtension.AggregateFunctionVariant]): ToAggregateFunction = {
    new ToAggregateFunction(functions) {
      override def getSigs: Seq[Sig] = FunctionMappings.AGGREGATE_SIGS
    }
  }

}
