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

import org.apache.arrow.vector.Float4Vector
import org.apache.arrow.vector.IntVector
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.ArrowType

import org.apache.spark.sql.execution.datasources.v2.arrow.SparkSchemaUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils

object CodeGeneration {
  private val defaultTimeZoneId = SparkSchemaUtils.getLocalTimezoneID()

  def getResultType(left: ArrowType, right: ArrowType): ArrowType = {
    //TODO(): remove this API
    left
  }

  def getResultType(dataType: DataType): ArrowType = {
    getResultType(dataType, defaultTimeZoneId)
  }

  def getResultType(dataType: DataType, timeZoneId: String): ArrowType = {
    dataType match {
      case other =>
        ArrowUtils.toArrowType(dataType, timeZoneId)
    }
    /*dataType match {
    case t: IntegerType =>
      new ArrowType.Int(32, true)
    case l: LongType =>
      new ArrowType.Int(64, true)
    case t: FloatType =>
      new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)
    case t: DoubleType =>
      new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
    case d: DecimalType =>
      new ArrowType.Decimal(d.precision, d.scale)
    case s: StringType =>
      new ArrowType.Utf8()
    case other =>
      throw new UnsupportedOperationException(s"getResultType doesn't support $other.")
      */
  }

  def getResultType(): ArrowType = {
    new ArrowType.Int(32, true)
  }

  def getPreciseLevel(dataType: ArrowType): Int = {
    dataType match {
      case t: ArrowType.Int =>
        4
      case t: ArrowType.FloatingPoint =>
        8
      case _ =>
        throw new UnsupportedOperationException(s"Unable to get precise level of $dataType ${dataType.getClass}.")
    }
  }

  def getCastFuncName(dataType: ArrowType): String = {
    dataType match {
      case t: ArrowType.FloatingPoint =>
        s"castFLOAT${4 * dataType.asInstanceOf[ArrowType.FloatingPoint].getPrecision().getFlatbufID()}"
      case _ =>
        throw new UnsupportedOperationException(s"getCastFuncName(${dataType}) is not supported.")
    }
  }
}
