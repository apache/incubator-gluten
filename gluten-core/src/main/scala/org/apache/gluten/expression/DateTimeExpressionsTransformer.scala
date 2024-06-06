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

import org.apache.gluten.exception.GlutenNotSupportException

import org.apache.spark.sql.catalyst.expressions._

/** The extract trait for 'GetDateField' from Date */
case class ExtractDateTransformer(
    substraitExprName: String,
    child: ExpressionTransformer,
    original: Expression)
  extends BinaryExpressionTransformer {
  override def left: ExpressionTransformer = {
    val dateFieldName =
      DateTimeExpressionsTransformer.EXTRACT_DATE_FIELD_MAPPING.get(original.getClass)
    if (dateFieldName.isEmpty) {
      throw new GlutenNotSupportException(s"$original not supported yet.")
    }
    LiteralTransformer(dateFieldName.get)
  }
  override def right: ExpressionTransformer = child
}

case class TruncTimestampTransformer(
    substraitExprName: String,
    format: ExpressionTransformer,
    timestamp: ExpressionTransformer,
    original: TruncTimestamp)
  extends ExpressionTransformer {
  override def children: Seq[ExpressionTransformer] = {
    val timeZoneId = original.timeZoneId.map(timeZoneId => LiteralTransformer(timeZoneId))
    Seq(format, timestamp) ++ timeZoneId
  }
}

case class MonthsBetweenTransformer(
    substraitExprName: String,
    date1: ExpressionTransformer,
    date2: ExpressionTransformer,
    roundOff: ExpressionTransformer,
    original: MonthsBetween)
  extends ExpressionTransformer {
  override def children: Seq[ExpressionTransformer] = {
    val timeZoneId = original.timeZoneId.map(timeZoneId => LiteralTransformer(timeZoneId))
    Seq(date1, date2, roundOff) ++ timeZoneId
  }
}

case class TimestampAddTransformer(
    substraitExprName: String,
    unit: String,
    left: ExpressionTransformer,
    right: ExpressionTransformer,
    timeZoneId: String,
    original: Expression)
  extends ExpressionTransformer {
  override def children: Seq[ExpressionTransformer] = {
    Seq(LiteralTransformer(unit), left, right, LiteralTransformer(timeZoneId))
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
