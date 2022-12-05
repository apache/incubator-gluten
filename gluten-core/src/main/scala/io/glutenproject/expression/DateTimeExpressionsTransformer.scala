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
import org.apache.spark.sql.catalyst.expressions.{CurrentDate, CurrentTimestamp, DateDiff, DayOfMonth, DayOfWeek, DayOfYear, Expression, Hour, MakeDate, MakeTimestamp, MicrosToTimestamp, MillisToTimestamp, Minute, Month, Now, Quarter, Second, SecondsToTimestamp, UnixDate, UnixMicros, UnixMillis, UnixSeconds, UnixTimestamp, WeekDay, WeekOfYear, Year}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

class CurrentTimestampTransformer() extends CurrentTimestamp with ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: CurrentTimestamp.")
  }
}

class CurrentDateTransformer(timeZoneId: Option[String] = None) extends CurrentDate(timeZoneId)
  with ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: CurrentDate.")
  }
}

class NowTransformer() extends Now()
  with ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: Now.")
  }
}


class HourTransformer(child: Expression, timeZoneId: Option[String] = None)
  extends Hour(child, timeZoneId) with ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: Hour.")
  }
}

class MinuteTransformer(child: Expression, timeZoneId: Option[String] = None)
  extends Minute(child, timeZoneId) with ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: Minute.")
  }
}

class SecondTransformer(child: Expression, timeZoneId: Option[String] = None)
   extends Second(child, timeZoneId) with ExtractDateTransformer {

  override def getField: String = "SECOND"

  override def getChild: Expression = child

  override def getDataType: DataType = dataType

//  override def doTransform(args: java.lang.Object): ExpressionNode = {
//    val childNode = getChild.asInstanceOf[ExpressionTransformer].doTransform(args)
//    if (!childNode.isInstanceOf[ExpressionNode]) {
//      throw new UnsupportedOperationException(s"not supported yet.")
//    }
//
//    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
//    val functionName = ConverterUtils.makeFuncName(
//      getField, Seq(getChild.dataType), FunctionConfig.OPT)
//    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
//    // val fieldNode = ExpressionBuilder.makeStringLiteral(getField)
//    val expressNodes = Lists.newArrayList(childNode.asInstanceOf[ExpressionNode])
//    val typeNode = ConverterUtils.getTypeNode(getDataType, getChild.nullable)
//
//    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
//  }
}

/**
 * The extract trait for 'GetDateField' from Date
 */
trait ExtractDateTransformer extends ExpressionTransformer {

  // mapping Substrait function name
  def getField: String

  def getChild: Expression

  def getDataType: DataType

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = getChild.asInstanceOf[ExpressionTransformer].doTransform(args)
    if (!childNode.isInstanceOf[ExpressionNode]) {
      throw new UnsupportedOperationException(s"not supported yet.")
    }

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = ConverterUtils.makeFuncName(
      ConverterUtils.EXTRACT, Seq(getChild.dataType), FunctionConfig.OPT)
    val functionId = ExpressionBuilder.newScalarFunction(functionMap, functionName)
    val fieldNode = ExpressionBuilder.makeStringLiteral(getField)
    val expressNodes = Lists.newArrayList(fieldNode, childNode.asInstanceOf[ExpressionNode])
    val typeNode = ConverterUtils.getTypeNode(getDataType, getChild.nullable)

    ExpressionBuilder.makeScalarFunction(functionId, expressNodes, typeNode)
  }
}

class DayOfMonthTransformer(child: Expression) extends DayOfMonth(child) with
  ExtractDateTransformer {
  override def getField: String = "DAY"

  override def getDataType: DataType = dataType

  override def getChild: Expression = child
}

class DayOfYearTransformer(child: Expression) extends DayOfYear(child) with
  ExtractDateTransformer {
  override def getField: String = "DAY_OF_YEAR"

  override def getDataType: DataType = dataType

  override def getChild: Expression = child

}

class DayOfWeekTransformer(child: Expression) extends DayOfWeek(child) with
  ExtractDateTransformer {
  override def getField: String = "DAY_OF_WEEK"

  override def getDataType: DataType = dataType

  override def getChild: Expression = child

}

class WeekDayTransformer(child: Expression) extends WeekDay(child) with
  ExtractDateTransformer {
  override def getField: String = "WEEK_DAY"

  override def getDataType: DataType = dataType

  override def getChild: Expression = child
}

class WeekOfYearTransformer(child: Expression) extends WeekOfYear(child) with
  ExtractDateTransformer {
  override def getField: String = "WEEK_OF_YEAR"

  override def getDataType: DataType = dataType

  override def getChild: Expression = child
}

class MonthTransformer(child: Expression) extends Month(child) with
  ExtractDateTransformer {
  override def getField: String = "MONTH"

  override def getDataType: DataType = dataType

  override def getChild: Expression = child
}

class QuarterTransformer(child: Expression) extends Quarter(child) with
  ExtractDateTransformer {
  override def getField: String = "QUARTER"

  override def getDataType: DataType = dataType

  override def getChild: Expression = child
}

class YearTransformer(child: Expression) extends Year(child) with
  ExtractDateTransformer {
  override def getField: String = "YEAR"

  override def getDataType: DataType = dataType

  override def getChild: Expression = child
}

class UnixDateTransformer(child: Expression) extends UnixDate(child) with
  ExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: UnixDate.")
  }
}

class UnixSecondsTransformer(child: Expression) extends UnixSeconds(child) with
  ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: UnixSeconds.")
  }
}

class UnixMillisTransformer(child: Expression) extends UnixMillis(child) with
  ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: UnixMillis.")
  }
}

class UnixMicrosTransformer(child: Expression) extends UnixMicros(child) with
  ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: UnixMicros.")
  }
}

class SecondsToTimestampTransformer(child: Expression) extends SecondsToTimestamp(child) with
  ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: SecondsToTimestamp.")
  }
}

class MillisToTimestampTransformer(child: Expression) extends MillisToTimestamp(child) with
  ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: MillisToTimestamp.")
  }
}

class MicrosToTimestampTransformer(child: Expression) extends MicrosToTimestamp(child) with
  ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: MicrosToTimestamp.")
  }
}

/**
 * Converts time string with given pattern to Unix time stamp (in seconds), returns null if fail.
 */
class UnixTimestampTransformer(left: Expression, right: Expression)
  extends UnixTimestamp(left, right) with
    ExpressionTransformer {
  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: UnixTimestamp.")
  }
}

class DateDiffTransformer(left: Expression, right: Expression)
  extends DateDiff(left, right) with ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: DateDiff.")
  }
}

class MakeDateTransformer(
                           year: Expression,
                           month: Expression,
                           day: Expression,
                           failOnError: Boolean = SQLConf.get.ansiEnabled)
  extends MakeDate(year, month, day, failOnError) with ExpressionTransformer {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: MakeDate.")
  }
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

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    throw new UnsupportedOperationException("Not supported: MakeTimestamp.")
  }
}

object DateTimeExpressionsTransformer {
  def castTimestampFromDate(child: Expression,
                            timeZoneId: Option[String] = None): ExpressionTransformer = {
    new CastTransformer(child, DateType, timeZoneId, null)
  }

  def castDateFromTimestamp(child: Expression,
                            timeZoneId: Option[String] = None): ExpressionTransformer = {
    new CastTransformer(child, TimestampType, timeZoneId, null)
  }
}
