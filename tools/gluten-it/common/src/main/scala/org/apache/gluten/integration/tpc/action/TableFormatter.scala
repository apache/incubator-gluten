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

package org.apache.gluten.integration.tpc.action

import java.io.{OutputStream, PrintStream}
import scala.collection.mutable

trait TableFormatter[ROW <: Any] {
  import TableFormatter._
  def appendRow(row: ROW): Unit
  def print(s: OutputStream): Unit
}

object TableFormatter {
  def create[ROW <: Any](fields: String*)(
      implicit parser: RowParser[ROW]): TableFormatter[ROW] = {
    assert(fields.nonEmpty)
    new Impl[ROW](Schema(fields), parser)
  }

  private case class Schema(fields: Seq[String])

  private class Impl[ROW <: Any](schema: Schema, parser: RowParser[ROW])
      extends TableFormatter[ROW] {
    private val rows = mutable.ListBuffer[Seq[String]]()

    override def appendRow(row: ROW): Unit = {
      val parsed = parser.parse(row)
      assert(parsed.size == schema.fields.size)
      rows += parsed.map(_.toString)
    }

    override def print(s: OutputStream): Unit = {
      val printer = new PrintStream(s)
      if(rows.isEmpty) {
        printer.println("(N/A)")
        printer.flush()
        return
      }
      val numFields = schema.fields.size
      val widths = (0 until numFields)
        .map { i =>
          rows.map(_(i).length).max max schema.fields(i).length
        }
        .map(_ + 1)
      val pBuilder = StringBuilder.newBuilder
      pBuilder ++= "|"
      widths.foreach { w =>
        pBuilder ++= s"%${w}s|"
      }
      val pattern = pBuilder.toString()
      printer.println(String.format(pattern, schema.fields: _*))
      rows.foreach { r =>
        printer.println(String.format(pattern, r: _*))
      }
      printer.flush()
    }
  }

  trait RowParser[ROW <: Any] {
    def parse(row: ROW): Seq[Any]
  }
}
