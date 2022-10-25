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
package io.substrait.spark

import io.substrait.spark.expression.{AggregateFunctionConverter, BinaryExpressionConverter, LiteralConverter}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.substrait.TypeConverter

import io.substrait.expression.{Expression => PExp, FieldReference}
import io.substrait.function.SimpleExtension

import scala.collection.JavaConverters

/** The builder to generate substrait expressions from catalyst expressions. */
abstract class ExpressionConverter {
  protected val binaryOperatorConverter: BinaryExpressionConverter

  def apply(e: Expression, output: Seq[Attribute] = Seq.empty): PExp = {
    convert(e, output).getOrElse(
      throw new UnsupportedOperationException(s"Unable to convert the expression $e"))
  }
  def convert(e: Expression, output: Seq[Attribute] = Seq.empty): Option[PExp] =
    generateExpression(e, output)

  private def generateExpression(expr: Expression, output: Seq[Attribute]): Option[PExp] =
    expr match {
      case a: AggregateExpression => None
      case l: Literal => LiteralConverter.convert(l)
      case a: AttributeReference if output.nonEmpty =>
        val bindReference =
          BindReferences.bindReference(expr, output, allowFailures = true)
        if (bindReference == a) {
          None
        } else {
          TypeConverter
            .convert(a.dataType, a.nullable)
            .map(
              FieldReference
                .newRootStructReference(bindReference.asInstanceOf[BoundReference].ordinal, _))
        }
      case a: Alias => generateExpression(a.child, output) // ?
      case b: BinaryExpression =>
        generateExpression(b.left, output).flatMap(
          left =>
            generateExpression(b.right, output).flatMap(
              right => binaryOperatorConverter.convert(b, left :: right :: Nil)))
      case u: UnaryExpression =>
        generateExpression(u.child, output)
          .flatMap(singleChild => binaryOperatorConverter.convert(u, singleChild :: Nil))
      case _ => None
    }
}

object ExpressionConverter {
  val EXTENSION_COLLECTION: SimpleExtension.ExtensionCollection = SimpleExtension.loadDefaults()

  val defaultConverter: ExpressionConverter = new ExpressionConverter {
    override protected val binaryOperatorConverter: BinaryExpressionConverter =
      BinaryExpressionConverter(
        JavaConverters.asScalaBuffer(EXTENSION_COLLECTION.scalarFunctions()))

    protected val aggregateConverter: AggregateFunctionConverter =
      AggregateFunctionConverter(
        JavaConverters.asScalaBuffer(EXTENSION_COLLECTION.aggregateFunctions()))
  }
}
