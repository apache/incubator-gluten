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

import org.apache.gluten.qt.QualificationToolConfiguration

case class PrimaryReportWriter(conf: QualificationToolConfiguration)
  extends ReportWriter[ApplicationReport](conf, "AppsRecommendedForBoost") {
  override def getHeader: String = Seq(
    "applicationId", // 0
    "applicationName", // 1
    "batchUuid", // 2
    "rddPercentage", // 3
    "unsupportedSqlPercentage", // 4
    "totalTaskTime", // 5
    "supportedTaskTime", // 6
    "supportedSqlPercentage", // 7
    "recommendedForBoost", // 8
    "expectedRuntimeReduction" // 9
  ).filter(_.nonEmpty).mkString("\t")

  override def doPostProcess(lines: Iterator[String]): Iterator[String] = {
    lines
      .map(_.split("\t").map(_.trim))
      .toSeq
      .sortBy(a => (a(8).toBoolean, a(6).toLong))(
        Ordering.Tuple2(Ordering.Boolean.reverse, Ordering.Long.reverse))
      .map(_.mkString("\t"))
      .toIterator
  }
}
