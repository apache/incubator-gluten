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
package org.apache.gluten.qt.writer

case class ApplicationReport(
    applicationId: String,
    applicationName: String,
    batchUuidd: String,
    totalTaskTime: Long,
    rddPercentage: Double,
    unsupportedSqlPercentage: Double,
    supportedSqlPercentage: Double
) extends Report {
  private def getSupportedTaskTime: Long = (totalTaskTime * supportedSqlPercentage).toLong

  private val SPEED_BOOST = 0.3

  override def toTSVLine: String = Seq(
    s"$applicationId",
    s"$applicationName",
    s"$batchUuidd",
    s"${round(rddPercentage * 100)}%",
    s"${round(unsupportedSqlPercentage * 100)}%",
    s"$totalTaskTime",
    s"$getSupportedTaskTime",
    s"${round(supportedSqlPercentage * 100)}%",
    s"${supportedSqlPercentage >= 0.7}",
    s"${round(supportedSqlPercentage * SPEED_BOOST * 100)}%"
  ).filter(_.nonEmpty).map(_.replaceAll("\t", " ")).mkString("\t")

  private def round(percentage: Double, points: Int = 1): BigDecimal =
    BigDecimal(Math.floor(percentage * Math.pow(10, points)) / Math.pow(10, points))
      .setScale(points, BigDecimal.RoundingMode.DOWN)
}
