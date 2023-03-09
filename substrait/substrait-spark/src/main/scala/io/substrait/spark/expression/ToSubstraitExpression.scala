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

import io.substrait.spark.HasOutputStack

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.substrait.{SparkTypeUtil, ToSubstraitType}
import org.apache.spark.unsafe.types.UTF8String

import io.substrait.expression.{Expression => SExpression, ExpressionCreator, FieldReference, ImmutableExpression}
import io.substrait.utils.Util

import scala.collection.JavaConverters.asJavaIterableConverter

/** The builder to generate substrait expressions from catalyst expressions. */
abstract class ToSubstraitExpression extends HasOutputStack[Seq[Attribute]] {

  object ScalarFunction {
    def unapply(e: Expression): Option[Seq[Expression]] = e match {
      case MakeDecimal(child, precision, scale, nullOnOverflow) =>
        Some(Seq(child, Literal(precision), Literal(scale), Literal(nullOnOverflow)))
      case BinaryExpression(left, right) => Some(Seq(left, right))
      case UnaryExpression(child) => Some(Seq(child))
      case t: TernaryExpression => Some(Seq(t.first, t.second, t.third))
      case _ => None
    }
  }

  type OutputT = Seq[Attribute]

  protected val toScalarFunction: ToScalarFunction

  protected def default(e: Expression): Option[SExpression] = {
    throw new UnsupportedOperationException(s"Unable to convert the expression $e")
  }

  def apply(e: Expression, output: OutputT = Nil): SExpression = {
    convert(e, output).getOrElse(
      throw new UnsupportedOperationException(s"Unable to convert the expression $e")
    )
  }
  def convert(expr: Expression, output: OutputT = Nil): Option[SExpression] = {
    pushOutput(output)
    try {
      translateUp(expr)
    } finally {
      popOutput()
    }
  }

  protected def translateSubQuery(expr: PlanExpression[_]): Option[SExpression] = default(expr)

  protected def translateAttribute(a: AttributeReference): Option[SExpression] = {
    val bindReference =
      BindReferences.bindReference[Expression](a, currentOutput, allowFailures = false)
    if (bindReference == a) {
      default(a)
    } else {
      Some(
        FieldReference.newRootStructReference(
          bindReference.asInstanceOf[BoundReference].ordinal,
          ToSubstraitType.apply(a.dataType, a.nullable))
      )
    }
  }

  protected def translateCaseWhen(
      branches: Seq[(Expression, Expression)],
      elseValue: Option[Expression]): Option[SExpression] = {
    val cases =
      for ((predicate, trueValue) <- branches)
        yield translateUp(predicate).flatMap(
          p =>
            translateUp(trueValue).map(
              t => {
                ImmutableExpression.IfClause.builder
                  .condition(p)
                  .`then`(t)
                  .build()
              }))
    val sparkElse = elseValue.getOrElse(Literal.create(null, branches.head._2.dataType))
    Util
      .seqToOption(cases.toList)
      .flatMap(
        caseConditions =>
          translateUp(sparkElse).map(
            defaultResult => {
              ExpressionCreator.ifThenStatement(defaultResult, caseConditions.asJava)
            }))
  }
  protected def translateIn(value: Expression, list: Seq[Expression]): Option[SExpression] = {
    Util
      .seqToOption(list.map(translateUp).toList)
      .flatMap(
        inList =>
          translateUp(value).map(
            inValue => {
              SExpression.SingleOrList
                .builder()
                .condition(inValue)
                .options(inList.asJava)
                .build()
            }))
  }

  protected def translateUp(expr: Expression): Option[SExpression] = {
    expr match {
      case c @ Cast(child, dataType, _, _) =>
        translateUp(child)
          .map(ExpressionCreator.cast(ToSubstraitType.apply(dataType, c.nullable), _))
      case c @ CheckOverflow(child, dataType, _) =>
        // CheckOverflow similar with cast
        translateUp(child)
          .map(
            childExpr => {
              if (SparkTypeUtil.sameType(dataType, child.dataType)) {
                childExpr
              } else {
                ExpressionCreator.cast(ToSubstraitType.apply(dataType, c.nullable), childExpr)
              }
            })
      case SubstraitLiteral(substraitLiteral) => Some(substraitLiteral)
      case a: AttributeReference if currentOutput.nonEmpty => translateAttribute(a)
      case a: Alias => translateUp(a.child)
      case p: PromotePrecision => translateUp(p.child)
      case CaseWhen(branches, elseValue) => translateCaseWhen(branches, elseValue)
      case In(value, list) => translateIn(value, list)
      case InSet(child, set) =>
        translateIn(
          child,
          set.map {
            case s: UTF8String => Literal(s, StringType)
            case other => Literal(other)
          }.toSeq)
      case scalar @ ScalarFunction(children) =>
        Util
          .seqToOption(children.map(translateUp))
          .flatMap(toScalarFunction.convert(scalar, _))
      case p: PlanExpression[_] => translateSubQuery(p)
      case other => default(other)
    }
  }
}
