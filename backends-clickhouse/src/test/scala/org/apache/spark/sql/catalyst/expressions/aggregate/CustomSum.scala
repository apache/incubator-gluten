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
package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.TreePattern.{SUM, TreePattern}
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Port the `Sum` aggregate function from Vanilla Spark 3.2, only changes class name to CustomSum.
 */
case class CustomSum(child: Expression, failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends DeclarativeAggregate
  with ImplicitCastInputTypes
  with UnaryLike[Expression] {

  def this(child: Expression) = this(child, failOnError = SQLConf.get.ansiEnabled)

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(NumericType, YearMonthIntervalType, DayTimeIntervalType))

  override def checkInputDataTypes(): TypeCheckResult = TypeCheckResult.TypeCheckSuccess

  final override val nodePatterns: Seq[TreePattern] = Seq(SUM)

  private lazy val resultType = child.dataType match {
    case DecimalType.Fixed(precision, scale) =>
      DecimalType.bounded(precision + 10, scale)
    case _: IntegralType => LongType
    case it: YearMonthIntervalType => it
    case it: DayTimeIntervalType => it
    case _ => DoubleType
  }

  private lazy val sum = AttributeReference("sum", resultType)()

  private lazy val isEmpty = AttributeReference("isEmpty", BooleanType, nullable = false)()

  private lazy val zero = Literal.default(resultType)

  override lazy val aggBufferAttributes = resultType match {
    case _: DecimalType => sum :: isEmpty :: Nil
    case _ => sum :: Nil
  }

  override lazy val initialValues: Seq[Expression] = resultType match {
    case _: DecimalType => Seq(zero, Literal(true, BooleanType))
    case _ => Seq(Literal(null, resultType))
  }

  override lazy val updateExpressions: Seq[Expression] = {
    resultType match {
      case _: DecimalType =>
        // For decimal type, the initial value of `sum` is 0. We need to keep `sum` unchanged if
        // the input is null, as SUM function ignores null input. The `sum` can only be null if
        // overflow happens under non-ansi mode.
        val sumExpr = if (child.nullable) {
          If(child.isNull, sum, sum + KnownNotNull(child).cast(resultType))
        } else {
          sum + child.cast(resultType)
        }
        // The buffer becomes non-empty after seeing the first not-null input.
        val isEmptyExpr = if (child.nullable) {
          isEmpty && child.isNull
        } else {
          Literal(false, BooleanType)
        }
        Seq(sumExpr, isEmptyExpr)
      case _ =>
        // For non-decimal type, the initial value of `sum` is null, which indicates no value.
        // We need `coalesce(sum, zero)` to start summing values. And we need an outer `coalesce`
        // in case the input is nullable. The `sum` can only be null if there is no value, as
        // non-decimal type can produce overflowed value under non-ansi mode.
        if (child.nullable) {
          Seq(coalesce(coalesce(sum, zero) + child.cast(resultType), sum))
        } else {
          Seq(coalesce(sum, zero) + child.cast(resultType))
        }
    }
  }

  /**
   * For decimal type: If isEmpty is false and if sum is null, then it means we have had an
   * overflow.
   *
   * update of the sum is as follows: Check if either portion of the left.sum or right.sum has
   * overflowed If it has, then the sum value will remain null. If it did not have overflow, then
   * add the sum.left and sum.right
   *
   * isEmpty: Set to false if either one of the left or right is set to false. This means we have
   * seen atleast a value that was not null.
   */
  override lazy val mergeExpressions: Seq[Expression] = {
    resultType match {
      case _: DecimalType =>
        val bufferOverflow = !isEmpty.left && sum.left.isNull
        val inputOverflow = !isEmpty.right && sum.right.isNull
        Seq(
          If(
            bufferOverflow || inputOverflow,
            Literal.create(null, resultType),
            // If both the buffer and the input do not overflow, just add them, as they can't be
            // null. See the comments inside `updateExpressions`: `sum` can only be null if
            // overflow happens.
            KnownNotNull(sum.left) + KnownNotNull(sum.right)
          ),
          isEmpty.left && isEmpty.right
        )
      case _ => Seq(coalesce(coalesce(sum.left, zero) + sum.right, sum.left))
    }
  }

  /**
   * If the isEmpty is true, then it means there were no values to begin with or all the values were
   * null, so the result will be null. If the isEmpty is false, then if sum is null that means an
   * overflow has happened. So now, if ansi is enabled, then throw exception, if not then return
   * null. If sum is not null, then return the sum.
   */
  override lazy val evaluateExpression: Expression = sum

  override protected def withNewChildInternal(newChild: Expression): CustomSum =
    copy(child = newChild)

  // The flag `failOnError` won't be shown in the `toString` or `toAggString` methods
  override def flatArguments: Iterator[Any] = Iterator(child)
}
