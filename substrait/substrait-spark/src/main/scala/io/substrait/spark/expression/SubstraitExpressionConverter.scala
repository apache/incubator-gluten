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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Expression, Literal}
import org.apache.spark.substrait.TypeConverter

import io.substrait.`type`.{StringTypeVisitor, Type}
import io.substrait.{expression => exp}
import io.substrait.expression.{Expression => SExpression}
import io.substrait.function.SimpleExtension

import scala.collection.JavaConverters.asScalaBufferConverter

class SubstraitExpressionConverter(
    val scalarFunctionConverter: BinaryExpressionConverter
) extends exp.AbstractExpressionVisitor[Expression, RuntimeException]
  with exp.FunctionArg.FuncArgVisitor[Expression, RuntimeException] {

  var output: Seq[Attribute] = Nil

  override def visitFallback(expr: SExpression): Expression =
    throw new UnsupportedOperationException(
      s"Expression $expr of type ${expr.getClass.getCanonicalName} " +
        s"not handled by visitor type ${getClass.getCanonicalName}.")

  override def visit(expr: SExpression.BoolLiteral): Expression = {
    if (expr.value()) {
      Literal.TrueLiteral
    } else {
      Literal.FalseLiteral
    }
  }
  override def visit(expr: SExpression.I32Literal): Expression = {
    Literal(expr.value(), TypeConverter.convert(expr.getType))
  }

  override def visit(expr: SExpression.I64Literal): Expression = {
    Literal(expr.value(), TypeConverter.convert(expr.getType))
  }

  override def visit(expr: SExpression.Cast): Expression = {
    val childExp = expr.input().accept(this)
    Cast(childExp, TypeConverter.convert(expr.getType))
  }

  override def visit(expr: exp.FieldReference): Expression = {
    if (expr.isSimpleRootReference) {
      val segment = expr.segments().get(0)
      segment match {
        case f: exp.FieldReference.StructField => output(f.offset()).clone()
        case _ => throw new IllegalArgumentException(s"Unhandled type: $segment")
      }
    } else {
      visitFallback(expr)
    }
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

  override def visitExpr(fnDef: SimpleExtension.Function, argIdx: Int, e: SExpression): Expression =
    e.accept(this)

  override def visitType(fnDef: SimpleExtension.Function, argIdx: Int, t: Type): Expression =
    throw new UnsupportedOperationException(
      s"FunctionArg $t not handled by visitor type ${getClass.getCanonicalName}.")

  override def visitEnumArg(
      fnDef: SimpleExtension.Function,
      argIdx: Int,
      e: exp.EnumArg): Expression =
    throw new UnsupportedOperationException(
      s"EnumArg(value=${e.value()}) not handled by visitor type ${getClass.getCanonicalName}.")
}
