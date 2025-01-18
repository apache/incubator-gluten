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

import java.io.{BufferedWriter, FileWriter}

import scala.io.Source

abstract class ReportWriter[T <: Report](conf: QualificationToolConfiguration, name: String) {
  private val tempPath = conf.outputPath.resolve(s"${name}_t.tsv")
  private val path = WriteUtils.getUniquePath(conf.outputPath.resolve(s"$name.tsv"))
  WriteUtils.deleteFileIfPresent(tempPath.toAbsolutePath.toString)
  WriteUtils.deleteFileIfPresent(path.toAbsolutePath.toString)
  private val writer: BufferedWriter = new BufferedWriter(
    new FileWriter(tempPath.toAbsolutePath.toString, true))
  addLine(getHeader)

  def getHeader: String

  def addReport(report: T): Unit = {
    addLine(report.toTSVLine)
  }

  def addReportBatch(reportBatch: Seq[T]): Unit = {
    reportBatch.foreach(addReport)
  }

  private def addLine(line: String): Unit = writer.synchronized {
    writer.write(line)
    writer.newLine()
    writer.flush()
  }

  def closeFile(): Unit = {
    if (writer != null) {
      writer.flush()
      writer.close()
    }
    postProcess()
  }

  def getFileString: String = path.toAbsolutePath.toString

  def doPostProcess(lines: Iterator[String]): Iterator[String]

  private def postProcess(): Unit = {
    val filename = tempPath.toAbsolutePath.toString
    val source = Source.fromFile(filename)
    try {
      val lines = source.getLines().drop(1)
      val newLines = doPostProcess(lines)
      val writer = new BufferedWriter(new FileWriter(path.toAbsolutePath.toString, true))
      writer.write(getHeader)
      writer.newLine()
      while (newLines.hasNext) {
        writer.write(newLines.next())
        writer.newLine()
      }
      writer.flush()
      writer.close()
    } finally {
      source.close()
    }
    WriteUtils.deleteFileIfPresent(filename)
  }
}
