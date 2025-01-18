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

import scala.collection.mutable

case class OperatorImpactReportWriter(conf: QualificationToolConfiguration)
  extends ReportWriter[ImpactReport](conf = conf, name = "UnsupportedOperators") {
  override def getHeader: String = Seq(
    "unsupportedOperator", // 0
    "cumulativeCpuMs", // 1
    "count" // 2
  ).mkString("\t")

  override def doPostProcess(lines: Iterator[String]): Iterator[String] = {
    val sumMap = mutable.Map[String, (Long, Long)]()
    for (line <- lines) {
      val Array(col1, col2, col3) = line.split("\t").map(_.trim)
      val cpu = col2.toLong
      val count = col3.toLong
      val current = sumMap.getOrElse(col1, (0L, 0L))
      sumMap(col1) = (current._1 + cpu, current._2 + count)
    }
    sumMap.toSeq
      .sortBy(_._2)(Ordering.Tuple2(Ordering.Long.reverse, Ordering.Long.reverse))
      .map(kv => s"${kv._1}\t${kv._2._1}\t${kv._2._2}")
      .toIterator
  }
}
