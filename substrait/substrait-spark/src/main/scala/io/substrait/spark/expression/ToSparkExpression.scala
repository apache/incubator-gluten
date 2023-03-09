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

import io.substrait.spark.{DefaultExpressionVisitor, HasOutputStack}
import io.substrait.spark.logical.ToLogicalPlan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.Decimal
import org.apache.spark.substrait.{SparkTypeUtil, ToSubstraitType}
import org.apache.spark.unsafe.types.UTF8String

import io.substrait.`type`.{StringTypeVisitor, Type}
import io.substrait.{expression => exp}
import io.substrait.expression.{Expression => SExpression}
import io.substrait.util.DecimalUtil

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.immutable.HashSet

class ToSparkExpression(
    val scalarFunctionConverter: ToScalarFunction,
    val toLogicalPlan: Option[ToLogicalPlan] = None)
  extends DefaultExpressionVisitor[Expression]
  with SQLConfHelper
  with HasOutputStack[Seq[NamedExpression]] {

  override def visit(expr: SExpression.BoolLiteral): Expression = {
    if (expr.value()) {
      Literal.TrueLiteral
    } else {
      Literal.FalseLiteral
    }
  }

  override def visit(expr: SExpression.NullLiteral): Expression = {
    Literal(null)
  }

  override def visit(expr: SExpression.I32Literal): Expression = {
    Literal(expr.value(), ToSubstraitType.convert(expr.getType))
  }

  override def visit(expr: SExpression.I64Literal): Expression = {
    Literal(expr.value(), ToSubstraitType.convert(expr.getType))
  }

  override def visit(expr: SExpression.FP64Literal): Expression = {
    Literal(expr.value(), ToSubstraitType.convert(expr.getType))
  }

  override def visit(expr: SExpression.StrLiteral): Expression = {
    Literal(UTF8String.fromString(expr.value()), ToSubstraitType.convert(expr.getType))
  }
  override def visit(expr: SExpression.DecimalLiteral): Expression = {
    val value = expr.value.toByteArray
    val decimal = DecimalUtil.getBigDecimalFromBytes(value, expr.scale, 16)
    Literal(Decimal(decimal), ToSubstraitType.convert(expr.getType))
  }
  override def visit(expr: SExpression.DateLiteral): Expression = {
    Literal(expr.value(), ToSubstraitType.convert(expr.getType))
  }

  override def visit(expr: SExpression.Cast): Expression = {
    val childExp = expr.input().accept(this)
    val dataType = ToSubstraitType.convert(expr.getType)
    if (Cast.needsTimeZone(childExp.dataType, dataType)) {
      Cast(childExp, dataType, Some(conf.sessionLocalTimeZone))
    } else {
      Cast(childExp, dataType)
    }
  }

  override def visit(expr: exp.FieldReference): Expression = {
    withFieldReference(expr)(i => currentOutput(i).clone())
  }
  override def visit(expr: SExpression.IfThen): Expression = {
    val branches = expr
      .ifClauses()
      .asScala
      .map(
        ifClause => {
          val predicate = ifClause.condition().accept(this)
          val elseValue = ifClause.`then`().accept(this)
          (predicate, elseValue)
        })
    val default = expr.elseClause().accept(this) match {
      case l: Literal if l.nullable => None
      case other => Some(other)
    }
    CaseWhen(branches, default)
  }

  override def visit(expr: SExpression.ScalarSubquery): Expression = {
    val rel = expr.input()
    val dataType = ToSubstraitType.convert(expr.getType)
    toLogicalPlan
      .map(
        relConverter => {
          val plan = relConverter.convert(rel)
          val result = ScalarSubquery(plan)
          SparkTypeUtil.sameType(result.dataType, dataType)
          result
        })
      .getOrElse(visitFallback(expr))
  }

  override def visit(expr: SExpression.SingleOrList): Expression = {
    val value = expr.condition().accept(this)
    val list = expr.options().asScala.map(e => e.accept(this))
    OptimizeIn(In(value, list))
  }
  override def visit(expr: SExpression.ScalarFunctionInvocation): Expression = {
    val eArgs = expr.arguments().asScala
    val args = eArgs.zipWithIndex.map {
      case (arg, i) =>
        arg.accept(expr.declaration(), i, this)
    }

    scalarFunctionConverter
      .getSparkExpressionFromSubstraitFunc(expr.declaration().key(), expr.outputType())
      .flatMap(sig => Option(sig.makeCall(args)))
      .getOrElse({
        val msg = String.format(
          "Unable to convert scalar function %s(%s).",
          expr.declaration.name,
          expr.arguments.asScala
            .map {
              case ea: exp.EnumArg => ea.value.toString
              case e: SExpression => e.getType.accept(new StringTypeVisitor)
              case t: Type => t.accept(new StringTypeVisitor)
              case a => throw new IllegalStateException("Unexpected value: " + a)
            }
            .mkString(", ")
        )
        throw new IllegalArgumentException(msg)
      })
  }
}

/**
 * Replaces [[In (value, seq[Literal])]] with optimized version [[InSet (value, HashSet[Literal])]]
 * which is much faster.
 */
object OptimizeIn extends SQLConfHelper with Logging {

  def apply(in: In): Expression = if (in.inSetConvertible) {
    val v = in.value
    val list = in.list
    val newList = ExpressionSet(list).toSeq
    if (
      newList.length == 1
      // TODO: `EqualTo` for structural types are not working. Until SPARK-24443 is addressed,
      // TODO: we exclude them in this rule.
      && !v.isInstanceOf[CreateNamedStruct]
      && !newList.head.isInstanceOf[CreateNamedStruct]
    ) {
      EqualTo(v, newList.head)
    } else if (newList.length > conf.optimizerInSetConversionThreshold) {
      val hSet = newList.map(e => e.eval(EmptyRow))
      InSet(v, HashSet() ++ hSet)
    } else if (newList.length < list.length) {
      in.copy(list = newList)
    } else { // newList.length == list.length && newList.length > 1
      in
    }
  } else {
    in
  }
}
