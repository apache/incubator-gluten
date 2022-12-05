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
import org.apache.spark.sql.catalyst.expressions.{DayOfMonth, DayOfWeek, DayOfYear, Expression, Month, Quarter, Second, WeekDay, WeekOfYear, Year}
import org.apache.spark.sql.types._

/**
 * The extract trait for 'GetDateField' from Date
 */
class ExtractDateTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression) extends ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"${original} not supported yet.")
    }

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
