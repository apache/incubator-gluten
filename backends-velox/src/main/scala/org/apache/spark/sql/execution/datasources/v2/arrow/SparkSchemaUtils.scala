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

package org.apache.spark.sql.execution.datasources.v2.arrow

import java.util.Objects
import java.util.TimeZone

import org.apache.arrow.vector.types.pojo.Schema

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils

object SparkSchemaUtils {

  def fromArrowSchema(schema: Schema): StructType = {
    ArrowUtils.fromArrowSchema(schema)
  }

  def toArrowSchema(schema: StructType, timeZoneId: String): Schema = {
    ArrowUtils.toArrowSchema(schema, timeZoneId)
  }

  @deprecated  // experimental
  def getGandivaCompatibleTimeZoneID(): String = {
    val zone = SQLConf.get.sessionLocalTimeZone
    validateGandivaCompatibleTimezoneID(zone)
    zone
  }

  def getLocalTimezoneID(): String = {
    SQLConf.get.sessionLocalTimeZone
  }

  def validateGandivaCompatibleTimezoneID(zoneId: String): Unit = {
    throw new UnsupportedOperationException("not implemented") // fixme 20210602 hongze
    if (!isTimeZoneIDGandivaCompatible(zoneId)) {
      throw new RuntimeException("Running Spark with Native SQL engine in non-UTC timezone" +
          " environment is forbidden. Consider setting session timezone within Spark config " +
          "spark.sql.session.timeZone. E.g. spark.sql.session.timeZone = UTC")
    }
  }

  def isTimeZoneIDGandivaCompatible(zoneId: String): Boolean = {
    // Only UTC supported by Gandiva kernels so far
    isTimeZoneIDEquivalentToUTC(zoneId)
  }

  def timeZoneIDEquals(one: String, other: String): Boolean = {
    getTimeZoneIDOffset(one) == getTimeZoneIDOffset(other)
  }

  def isTimeZoneIDEquivalentToUTC(zoneId: String): Boolean = {
    getTimeZoneIDOffset(zoneId) == 0
  }

  def getTimeZoneIDOffset(zoneId: String): Int = {
    Objects.requireNonNull(zoneId)
    TimeZone.getTimeZone(zoneId)
        .toZoneId
        .getRules
        .getOffset(java.time.Instant.now())
        .getTotalSeconds
  }
}
