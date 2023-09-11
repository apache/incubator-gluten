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

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import com.google.common.collect.Lists

/** The extract trait for 'GetDateField' from Date */
case class ExtractDateTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression)
  extends ExpressionTransformer {

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
      throw new UnsupportedOperationException(s"$original not supported yet.")
    }
    val fieldNode = ExpressionBuilder.makeStringLiteral(dateFieldName.get)
    val expressNodes = Lists.newArrayList(fieldNode, childNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

case class DateDiffTransformer(
    substraitExprName: String,
    endDate: ExpressionTransformer,
    startDate: ExpressionTransformer,
    original: DateDiff)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val endDateNode = endDate.doTransform(args)
    val startDateNode = startDate.doTransform(args)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      substraitExprName,
      Seq(StringType, original.startDate.dataType, original.endDate.dataType),
      FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)

    val expressionNodes = if (BackendsApiManager.isCHBackend) {
      // In CH backend, datediff params are ('day', startDate, endDate).
      Lists.newArrayList(ExpressionBuilder.makeStringLiteral("day"), startDateNode, endDateNode)
    } else {
      // In the others, datediff params are (endDate, startDate).
      Lists.newArrayList(endDateNode, startDateNode)
    }
    ExpressionBuilder.makeScalarFunction(
      functionId,
      expressionNodes,
      ConverterUtils.getTypeNode(original.dataType, original.nullable))
  }
}

case class FromUnixTimeTransformer(
    substraitExprName: String,
    sec: ExpressionTransformer,
    format: ExpressionTransformer,
    timeZoneId: Option[String] = None,
    original: FromUnixTime)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val secNode = sec.doTransform(args)
    val formatNode = format.doTransform(args)

    val dataTypes = if (timeZoneId != None) {
      Seq(original.sec.dataType, original.format.dataType, StringType)
    } else {
      Seq(original.sec.dataType, original.format.dataType)
    }
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
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

/**
 * The failOnError depends on the config for ANSI. ANSI is not supported currently. And timeZoneId
 * is passed to backend config.
 */
case class ToUnixTimestampTransformer(
    substraitExprName: String,
    timeExp: ExpressionTransformer,
    format: ExpressionTransformer,
    timeZoneId: Option[String],
    failOnError: Boolean,
    original: ToUnixTimestamp)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val dataTypes = Seq(original.timeExp.dataType, StringType)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(substraitExprName, dataTypes))

    val expressionNodes = new java.util.ArrayList[ExpressionNode]()
    val timeExpNode = timeExp.doTransform(args)
    expressionNodes.add(timeExpNode)
    val formatNode = format.doTransform(args)
    expressionNodes.add(formatNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

case class TruncTimestampTransformer(
    substraitExprName: String,
    format: ExpressionTransformer,
    timestamp: ExpressionTransformer,
    timeZoneId: Option[String] = None,
    original: TruncTimestamp)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val timestampNode = timestamp.doTransform(args)
    val formatNode = format.doTransform(args)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val dataTypes = if (timeZoneId != None) {
      Seq(original.format.dataType, original.timestamp.dataType, StringType)
    } else {
      Seq(original.format.dataType, original.timestamp.dataType)
    }

    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(substraitExprName, dataTypes))

    val expressionNodes = new java.util.ArrayList[ExpressionNode]()
    expressionNodes.add(formatNode)
    expressionNodes.add(timestampNode)
    if (timeZoneId != None) {
      expressionNodes.add(ExpressionBuilder.makeStringLiteral(timeZoneId.get))
    }

    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

case class MonthsBetweenTransformer(
    substraitExprName: String,
    date1: ExpressionTransformer,
    date2: ExpressionTransformer,
    roundOff: ExpressionTransformer,
    timeZoneId: Option[String] = None,
    original: MonthsBetween)
  extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val date1Node = date1.doTransform(args)
    val data2Node = date2.doTransform(args)
    val roundOffNode = roundOff.doTransform(args)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val dataTypes = if (timeZoneId != None) {
      Seq(original.date1.dataType, original.date2.dataType, original.roundOff.dataType, StringType)
    } else {
      Seq(original.date1.dataType, original.date2.dataType, original.roundOff.dataType)
    }

    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(substraitExprName, dataTypes))

    val expressionNodes = new java.util.ArrayList[ExpressionNode]()
    expressionNodes.add(date1Node)
    expressionNodes.add(data2Node)
    expressionNodes.add(roundOffNode)
    if (timeZoneId != None) {
      expressionNodes.add(ExpressionBuilder.makeStringLiteral(timeZoneId.get))
    }

    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

object DateTimeExpressionsTransformer {

  val EXTRACT_DATE_FIELD_MAPPING: Map[Class[_], String] = Map(
    scala.reflect.classTag[Year].runtimeClass -> "YEAR",
    scala.reflect.classTag[YearOfWeek].runtimeClass -> "YEAR_OF_WEEK",
    scala.reflect.classTag[Quarter].runtimeClass -> "QUARTER",
    scala.reflect.classTag[Month].runtimeClass -> "MONTH",
    scala.reflect.classTag[WeekOfYear].runtimeClass -> "WEEK_OF_YEAR",
    scala.reflect.classTag[WeekDay].runtimeClass -> "WEEK_DAY",
    scala.reflect.classTag[DayOfWeek].runtimeClass -> "DAY_OF_WEEK",
    scala.reflect.classTag[DayOfMonth].runtimeClass -> "DAY",
    scala.reflect.classTag[DayOfYear].runtimeClass -> "DAY_OF_YEAR",
    scala.reflect.classTag[Hour].runtimeClass -> "HOUR",
    scala.reflect.classTag[Minute].runtimeClass -> "MINUTE",
    scala.reflect.classTag[Second].runtimeClass -> "SECOND"
  )
}
