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

package org.apache.gluten.integration.action

import org.apache.gluten.integration.action.TableRender.RowParser.FieldAppender.RowAppender

import java.io.{OutputStream, PrintStream}
import scala.collection.mutable

trait TableRender[ROW <: Any] {
  def appendRow(row: ROW): Unit
  def print(s: OutputStream): Unit
}

object TableRender {
  def create[ROW <: Any](fields: Field*)(implicit parser: RowParser[ROW]): TableRender[ROW] = {
    assert(fields.nonEmpty)
    new Impl[ROW](Schema(fields), parser)
  }

  def plain[ROW <: Any](fields: String*)(implicit parser: RowParser[ROW]): TableRender[ROW] = {
    assert(fields.nonEmpty)
    new Impl[ROW](Schema(fields.map(Field.Leaf)), parser)
  }

  trait Field {
    def name: String
  }

  object Field {
    case class Branch(override val name: String, fields: Seq[Field]) extends Field
    case class Leaf(override val name: String) extends Field
  }

  private case class Schema(fields: Seq[Field]) {
    val leafs: Seq[Field.Leaf] = {
      fields.map(leafsOf).reduce(_ ++ _)
    }

    def numLeafs(): Int = {
      leafs.size
    }

    private def leafsOf(field: Field): Seq[Field.Leaf] = {
      field match {
        case l: Field.Leaf => List(l)
        case Field.Branch(_, fields) => fields.map(leafsOf).reduce(_ ++ _)
      }
    }
  }

  private class Impl[ROW <: Any](schema: Schema, parser: RowParser[ROW])
      extends TableRender[ROW] {
    private val appenderFactory = RowParser.FieldAppender.TableAppender.create(schema)

    override def appendRow(row: ROW): Unit = {
      parser.parse(appenderFactory.newRow(), row)
    }

    override def print(s: OutputStream): Unit = {
      val data = appenderFactory.data()
      val printer = new PrintStream(s)
      if (data.isEmpty) {
        printer.println("(N/A)")
        printer.flush()
        return
      }
      val widths = (0 until schema.numLeafs())
        .map { i =>
          data.map(_(i).length).max max schema.leafs(i).name.length
        }
        .map(_ + 1)
      val pBuilder = StringBuilder.newBuilder
      pBuilder ++= "|"
      widths.foreach { w =>
        pBuilder ++= s"%${w}s|"
      }
      val pattern = pBuilder.toString()
      printer.println(String.format(pattern, schema.leafs.map(_.name): _*))
      data.foreach { r =>
        printer.println(String.format(pattern, r: _*))
      }
      printer.flush()
    }
  }

  trait RowParser[ROW <: Any] {
    def parse(rowFactory: RowAppender, row: ROW): Unit
  }

  object RowParser {
    trait FieldAppender {
      def child(name: String): FieldAppender
      def write(value: Any): Unit
    }

    object FieldAppender {
      trait RowAppender {
        def field(name: String): FieldAppender
        def field(offset: Int): FieldAppender
        def incremental(): RowAppender.Incremental
      }

      object RowAppender {
        def create(
            schema: Schema,
            mutableRows: mutable.ListBuffer[Array[String]]): RowAppender = {
          new RowAppenderImpl(schema, mutableRows)
        }

        trait Incremental {
          def next(): FieldAppender
        }

        private class RowAppenderImpl(
            schema: Schema,
            mutableRows: mutable.ListBuffer[Array[String]])
            extends RowAppender {
          private val mutableRow = Array.tabulate(schema.numLeafs()) { _ =>
            "UNFILLED"
          }
          mutableRows += mutableRow

          override def field(name: String): FieldAppender = {
            val fields = schema.fields
            assert(fields.count(_.name == name) == 1)
            val field = fields.zipWithIndex.find(_._1.name == name).getOrElse {
              throw new IllegalArgumentException(s"Field $name not found in $schema")
            }
            val column = field._2
            new FieldAppenderImpl(field._1, mutableRow, column)
          }

          override def field(offset: Int): FieldAppender = {
            new FieldAppenderImpl(schema.fields(offset), mutableRow, offset)
          }

          override def incremental(): Incremental = {
            new Incremental {
              private var offset = 0
              override def next(): FieldAppender = {
                val out = field(offset)
                offset += 1
                out
              }
            }
          }
        }
      }

      trait TableAppender {
        def newRow(): RowAppender
        def data(): Seq[Seq[String]]
      }

      object TableAppender {
        def create(schema: Schema): TableAppender = {
          new TableAppenderImpl(schema)
        }

        private class TableAppenderImpl(schema: Schema) extends TableAppender {
          private val mutableRows: mutable.ListBuffer[Array[String]] = mutable.ListBuffer()

          override def newRow(): RowAppender = {
            RowAppender.create(schema, mutableRows)
          }

          override def data(): Seq[Seq[String]] = {
            mutableRows.map(_.toSeq)
          }
        }
      }

      private class FieldAppenderImpl(field: Field, mutableRow: Array[String], column: Int)
          extends FieldAppender {
        override def child(name: String): FieldAppender = {
          field match {
            case Field.Branch(_, fields) =>
              assert(fields.count(_.name == name) == 1)
              val child = fields.zipWithIndex.find(_._1.name == name).getOrElse {
                throw new IllegalArgumentException(s"Field $name not found in $field")
              }
              val childField = child._1
              val childOffset = child._2
              new FieldAppenderImpl(childField, mutableRow, column + childOffset)
            case _ =>
              throw new IllegalArgumentException(s"Field $field is not a branch")
          }
        }

        override def write(value: Any): Unit = {
          assert(field.isInstanceOf[Field.Leaf])
          mutableRow(column) = value.toString
        }
      }
    }
  }
}
