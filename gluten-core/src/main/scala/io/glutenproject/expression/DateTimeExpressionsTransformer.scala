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

package io.glutenproject.expression

import com.google.common.collect.Lists
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.internal.Logging
import io.glutenproject.substrait.expression.StringLiteralNode

/**
 * The extract trait for 'GetDateField' from Date
 */
class ExtractDateTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression) extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.doTransform(args)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      substraitExprName,
      original.children.map(_.dataType),
      FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val dateFieldName =
      DateTimeExpressionsTransformer.EXTRACT_DATE_FIELD_MAPPING.get(original.getClass)
    if (dateFieldName.isEmpty) {
      throw new UnsupportedOperationException(s"${original} not supported yet.")
    }
    val fieldNode = ExpressionBuilder.makeStringLiteral(dateFieldName.get)
    val expressNodes = Lists.newArrayList(fieldNode, childNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

class DateDiffTransformer(substraitExprName: String, endDate: ExpressionTransformer,
  startDate: ExpressionTransformer, original: DateDiff)
  extends ExpressionTransformer with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // In Spark: datediff(endDate, startDate)
    // In CH: date_diff('day', startDate, endDate)
    val endDateNode = endDate.doTransform(args)
    val startDateNode = startDate.doTransform(args)
    val unitNode = ExpressionBuilder.makeStringLiteral("day")

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      substraitExprName, Seq(StringType, original.startDate.dataType,
      original.endDate.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val expressionNodes = Lists.newArrayList(
      unitNode, startDateNode, endDateNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class FromUnixTimeTransformer(substraitExprName: String, sec: ExpressionTransformer,
  format: ExpressionTransformer, timeZoneId: Option[String] = None, original: FromUnixTime)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val secNode = sec.doTransform(args)
    var formatNode = format.doTransform(args)

    // Only when format = 'yyyy-MM-dd HH:mm:ss' can we transfrom the expr to substrait.
    if (!formatNode.isInstanceOf[StringLiteralNode] ||
      formatNode.asInstanceOf[StringLiteralNode].getValue != "yyyy-MM-dd HH:mm:ss") {
      throw new UnsupportedOperationException(s"not supported yet.")
    }
    formatNode = ExpressionBuilder.makeStringLiteral("%Y-%m-%d %R:%S")

    val dataTypes = if (timeZoneId != None) {
      Seq(original.sec.dataType, original.format.dataType, StringType)
    } else {
      Seq(original.sec.dataType, original.format.dataType)
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(substraitExprName, dataTypes))

    val expressionNodes = new java.util.ArrayList[ExpressionNode]()
    expressionNodes.add(secNode)
    expressionNodes.add(formatNode)
    if (timeZoneId != None) {
      expressionNodes.add(ExpressionBuilder.makeStringLiteral(timeZoneId.get))
    }

    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

case class ToUnixTimestampTransformer(substraitExprName: String, timeExp: ExpressionTransformer,
  format: ExpressionTransformer, timeZoneId: Option[String], failOnError: Boolean,
  original: ToUnixTimestamp)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // Only when timeExp is not string type or format = 'yyyy-MM-dd HH:mm:ss'
    // can we transfrom the expr to substrait.
    val formatNode = format.doTransform(args)
    if (original.timeExp.dataType.isInstanceOf[StringType] &&
      (!formatNode.isInstanceOf[StringLiteralNode] ||
      formatNode.asInstanceOf[StringLiteralNode].getValue != "yyyy-MM-dd HH:mm:ss")) {
      throw new UnsupportedOperationException(s"$original not supported yet.")
    }

    val dataTypes = if (timeZoneId != None) {
      Seq(original.timeExp.dataType, StringType)
    } else {
      Seq(original.timeExp.dataType)
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(functionMap,
      ConverterUtils.makeFuncName(substraitExprName, dataTypes))

    val expressionNodes = new java.util.ArrayList[ExpressionNode]()
    val timeExpNode = timeExp.doTransform(args)
    expressionNodes.add(timeExpNode)
    if (timeZoneId != None) {
      expressionNodes.add(ExpressionBuilder.makeStringLiteral(timeZoneId.get))
    }

    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class UnixTimestampTransformer(substraitExprName: String, timeExp: ExpressionTransformer,
  format: ExpressionTransformer, timeZoneId: Option[String], failOnError: Boolean,
  original: UnixTimestamp)
  extends ExpressionTransformer
  with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val toUnixTimestamp = ToUnixTimestamp(original.timeExp, original.format,
      original.timeZoneId, original.failOnError)
    val transformer = ToUnixTimestampTransformer(substraitExprName, timeExp, format,
      timeZoneId, failOnError, toUnixTimestamp)
    transformer.doTransform(args)
  }
}

object DateTimeExpressionsTransformer {

  val EXTRACT_DATE_FIELD_MAPPING: Map[Class[_], String] = Map(
    scala.reflect.classTag[Year].runtimeClass -> "YEAR",
    scala.reflect.classTag[Quarter].runtimeClass -> "QUARTER",
    scala.reflect.classTag[Month].runtimeClass -> "MONTH",
    scala.reflect.classTag[WeekOfYear].runtimeClass -> "WEEK_OF_YEAR",
    scala.reflect.classTag[WeekDay].runtimeClass -> "WEEK_DAY",
    scala.reflect.classTag[DayOfWeek].runtimeClass -> "DAY_OF_WEEK",
    scala.reflect.classTag[DayOfMonth].runtimeClass -> "DAY",
    scala.reflect.classTag[DayOfYear].runtimeClass -> "DAY_OF_YEAR",
    scala.reflect.classTag[Second].runtimeClass -> "SECOND"
  )
}
