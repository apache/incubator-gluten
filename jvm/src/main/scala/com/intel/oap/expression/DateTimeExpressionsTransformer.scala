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

package com.intel.oap.expression

import java.util.Collections

import com.google.common.collect.Lists
import com.intel.oap.expression.DateTimeExpressionsTransformer.castDateFromTimestamp
import com.intel.oap.substrait.expression.ExpressionNode
import org.apache.arrow.gandiva.expression.TreeBuilder
import org.apache.arrow.gandiva.expression.TreeNode
import org.apache.arrow.vector.types.DateUnit
import org.apache.arrow.vector.types.pojo.ArrowType
import org.apache.spark.sql.catalyst.expressions.CheckOverflow
import org.apache.spark.sql.catalyst.expressions.CurrentDate
import org.apache.spark.sql.catalyst.expressions.CurrentTimestamp
import org.apache.spark.sql.catalyst.expressions.DateDiff
import org.apache.spark.sql.catalyst.expressions.DayOfMonth
import org.apache.spark.sql.catalyst.expressions.DayOfWeek
import org.apache.spark.sql.catalyst.expressions.DayOfYear
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Hour
import org.apache.spark.sql.catalyst.expressions.MakeDate
import org.apache.spark.sql.catalyst.expressions.MakeTimestamp
import org.apache.spark.sql.catalyst.expressions.MicrosToTimestamp
import org.apache.spark.sql.catalyst.expressions.MillisToTimestamp
import org.apache.spark.sql.catalyst.expressions.Minute
import org.apache.spark.sql.catalyst.expressions.Month
import org.apache.spark.sql.catalyst.expressions.Now
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.sql.catalyst.expressions.SecondsToTimestamp
import org.apache.spark.sql.catalyst.expressions.UnixDate
import org.apache.spark.sql.catalyst.expressions.UnixMicros
import org.apache.spark.sql.catalyst.expressions.UnixMillis
import org.apache.spark.sql.catalyst.expressions.UnixSeconds
import org.apache.spark.sql.catalyst.expressions.UnixTimestamp
import org.apache.spark.sql.catalyst.expressions.Year
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType, TimestampType}
import org.apache.spark.sql.util.ArrowUtils

object DateTimeExpressionsTransformer {
  class CurrentTimestampTransformer() extends CurrentTimestamp with ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class CurrentDateTransformer(timeZoneId: Option[String] = None) extends CurrentDate(timeZoneId)
      with ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class NowTransformer() extends Now()
      with ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class HourTransformer(child: Expression, timeZoneId: Option[String] = None)
    extends Hour(child, timeZoneId) with ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class MinuteTransformer(child: Expression, timeZoneId: Option[String] = None)
    extends Minute(child, timeZoneId) with ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class SecondTransformer(child: Expression, timeZoneId: Option[String] = None)
    extends Second(child, timeZoneId) with ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class DayOfMonthTransformer(child: Expression) extends DayOfMonth(child) with
      ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class DayOfYearTransformer(child: Expression) extends DayOfYear(child) with
      ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class DayOfWeekTransformer(child: Expression) extends DayOfWeek(child) with
      ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class MonthTransformer(child: Expression) extends Month(child) with
      ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class YearTransformer(child: Expression) extends Year(child) with
      ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class UnixDateTransformer(child: Expression) extends UnixDate(child) with
      ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class UnixSecondsTransformer(child: Expression) extends UnixSeconds(child) with
      ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class UnixMillisTransformer(child: Expression) extends UnixMillis(child) with
      ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class UnixMicrosTransformer(child: Expression) extends UnixMicros(child) with
      ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class SecondsToTimestampTransformer(child: Expression) extends SecondsToTimestamp(child) with
      ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class MillisToTimestampTransformer(child: Expression) extends MillisToTimestamp(child) with
      ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class MicrosToTimestampTransformer(child: Expression) extends MicrosToTimestamp(child) with
      ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  /**
   * Converts time string with given pattern to Unix time stamp (in seconds), returns null if fail.
   */
  class UnixTimestampTransformer(left: Expression, right: Expression)
      extends UnixTimestamp(left, right) with
      ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class DateDiffTransformer(left: Expression, right: Expression)
      extends DateDiff(left, right) with ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  class MakeDateTransformer(
      year: Expression,
      month: Expression,
      day: Expression,
      failOnError: Boolean = SQLConf.get.ansiEnabled)
      extends MakeDate(year, month, day, failOnError) with ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }
  class MakeTimestampTransformer(
      year: Expression,
      month: Expression,
      day: Expression,
      hour: Expression,
      min: Expression,
      sec: Expression,
      timezone: Option[Expression] = None,
      timeZoneId: Option[String] = None,
      failOnError: Boolean = SQLConf.get.ansiEnabled)
      extends MakeTimestamp(year, month, day, hour, min, sec, timezone, timeZoneId, failOnError)
          with ExpressionTransformer {
    override def doTransform(args: java.lang.Object): ExpressionNode = null
  }

  def castTimestampFromDate(child: Expression,
      timeZoneId: Option[String] = None): ExpressionTransformer = {
    new CastTransformer(child, DateType, timeZoneId, null)
  }

  def castDateFromTimestamp(child: Expression,
      timeZoneId: Option[String] = None): ExpressionTransformer = {
    new CastTransformer(child, TimestampType, timeZoneId, null)
  }
}
