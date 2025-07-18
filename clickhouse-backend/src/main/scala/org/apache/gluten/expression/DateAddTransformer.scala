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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class DateAddTransformer(
    attributeSeq: Seq[Attribute],
    substraitExprName: String,
    children: Seq[Expression],
    expr: Expression)
  extends Logging {

  def doTransform(): ExpressionTransformer = {
    expr match {
      case dateAdd: DateAdd =>
        doDateAddTransform()
      case timeAdd: TimeAdd =>
        doTimeAddTransform()
      case other => doDefaultTransorm()
    }
  }

  private def unwrapExtractANSIIntervalDays(extractExpr: Expression): Expression = {
    extractExpr match {
      case extractDays: ExtractANSIIntervalDays =>
        extractDays.child match {
          case literal: Literal if literal.dataType.isInstanceOf[DayTimeIntervalType] =>
            val (intVal, unitVal) = parseDayIntervalType(literal)
            if (unitVal.equals("DAY")) {
              Literal(intVal, IntegerType)
            } else {
              null
            }
          case _ => null
        }
      case _ => null
    }
  }

  private def doDateAddTransform(): ExpressionTransformer = {
    children(1) match {
      case extractDays: ExtractANSIIntervalDays =>
        val daysLiteral = unwrapExtractANSIIntervalDays(children(1))
        if (daysLiteral != null) {
          val daysExpr = LiteralTransformer(daysLiteral)
          val dateExpr =
            ExpressionConverter.replaceWithExpressionTransformer(children(0), attributeSeq)
          GenericExpressionTransformer(substraitExprName, Seq(dateExpr, daysExpr), expr)
        } else {
          doDefaultTransorm()
        }
      case minus: UnaryMinus =>
        val child = minus.child match {
          case extractDays: ExtractANSIIntervalDays =>
            unwrapExtractANSIIntervalDays(extractDays)
          case e => e
        }
        if (child != null) {
          val newMinus = minus.withNewChildren(Seq(child))
          val dateExpr =
            ExpressionConverter.replaceWithExpressionTransformer(children(0), attributeSeq)
          val minusExpr =
            ExpressionConverter.replaceWithExpressionTransformer(newMinus, attributeSeq)
          GenericExpressionTransformer(substraitExprName, Seq(dateExpr, minusExpr), expr)
        } else {
          doDefaultTransorm()
        }
      case _ => doDefaultTransorm()
    }
  }

  private def doTimeAddTransform(): ExpressionTransformer = {
    children(1) match {
      case literal: Literal if literal.dataType.isInstanceOf[DayTimeIntervalType] =>
        val (intVal, unitVal) = parseDayIntervalType(literal)
        if (unitVal != null) {
          val timeExpr =
            ExpressionConverter.replaceWithExpressionTransformer(children(0), attributeSeq)

          val intExpr = Literal(intVal, IntegerType)
          val unitExpr = Literal(UTF8String.fromString(unitVal), StringType)
          val newExpr = expr.withNewChildren(Seq(unitExpr, intExpr))
          GenericExpressionTransformer(
            substraitExprName,
            Seq(LiteralTransformer(unitExpr), LiteralTransformer(intVal), timeExpr),
            newExpr)
        } else {
          doDefaultTransorm()
        }
      case minus: UnaryMinus =>
        minus.child match {
          case literal: Literal if literal.dataType.isInstanceOf[DayTimeIntervalType] =>
            val (intVal, unitVal) = parseDayIntervalType(literal)
            if (unitVal != null) {
              val intExpr = minus.withNewChildren(Seq(Literal(intVal, IntegerType)))
              val unitExpr = Literal(UTF8String.fromString(unitVal), StringType)
              val newExpr = expr.withNewChildren(Seq(unitExpr, intExpr))
              GenericExpressionTransformer(
                substraitExprName,
                Seq(LiteralTransformer(unitExpr), LiteralTransformer(intVal)),
                newExpr)
            } else {
              doDefaultTransorm()
            }
          case _ => doDefaultTransorm()
        }
      case _ => doDefaultTransorm()
    }
  }

  private def doDefaultTransorm(): ExpressionTransformer = {
    // transorm it in a generic way
    val childrenTransformers =
      children.map(ExpressionConverter.replaceWithExpressionTransformer(_, attributeSeq))
    GenericExpressionTransformer(substraitExprName, childrenTransformers, expr)
  }

  private def parseDayIntervalType(literal: Literal): (Integer, String) = {
    literal.dataType match {
      case dayIntervalType: DayTimeIntervalType =>
        val intVal = literal.value.asInstanceOf[Long]
        (dayIntervalType.startField, dayIntervalType.endField) match {
          case (0, 0) => ((literal.value.asInstanceOf[Long] / 1000000 / 3600 / 24).toInt, "DAY")
          case (1, 1) => ((literal.value.asInstanceOf[Long] / 1000000 / 3600).toInt, "HOUR")
          case (2, 2) => ((literal.value.asInstanceOf[Long] / 1000000L / 60L).toInt, "MINUTE")
          case (3, 3) => (literal.value.asInstanceOf[Long].toInt / 1000000, "SECOND")
          case _ => (0, null)
        }
      case _ => (0, null)
    }
  }
}
