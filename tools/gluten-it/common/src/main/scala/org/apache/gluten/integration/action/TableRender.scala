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

import org.apache.commons.lang3.StringUtils
import org.apache.gluten.integration.action.TableRender.RowParser.FieldAppender.RowAppender

import java.io.{ByteArrayOutputStream, OutputStream, PrintStream}
import scala.collection.mutable

trait TableRender[ROW <: Any] {
  def appendRow(row: ROW): Unit
  def print(s: OutputStream): Unit
}

object TableRender {
  def create[ROW <: Any](fields: Field*)(implicit parser: RowParser[ROW]): TableRender[ROW] = {
    assert(fields.nonEmpty)
    // Deep copy to avoid duplications (In case caller reuses a sub-tree).
    new Impl[ROW](Schema(fields.map(_.makeCopy())), parser)
  }

  def plain[ROW <: Any](fields: String*)(implicit parser: RowParser[ROW]): TableRender[ROW] = {
    assert(fields.nonEmpty)
    new Impl[ROW](Schema(fields.map(Field.Leaf)), parser)
  }

  trait Field {
    def id(): Int = System.identityHashCode(this)
    def name: String
    def leafs: Seq[Field.Leaf]
    def makeCopy(): Field
  }

  object Field {
    case class Branch(override val name: String, children: Seq[Field]) extends Field {
      override val leafs: Seq[Leaf] = {
        children.map(leafsOf).reduce(_ ++ _)
      }

      private def leafsOf(field: Field): Seq[Field.Leaf] = {
        field match {
          case l @ Field.Leaf(_) => List(l)
          case b @ Field.Branch(_, children) =>
            children.map(child => leafsOf(child)).reduce(_ ++ _)
        }
      }

      override def makeCopy(): Field = copy(name, children.map(_.makeCopy()))
    }
    case class Leaf(override val name: String) extends Field {
      override val leafs: Seq[Leaf] = List(this)
      override def makeCopy(): Field = copy()
    }
  }

  private case class Schema(fields: Seq[Field]) {
    val leafs: Seq[Field.Leaf] = {
      fields.map(_.leafs).reduce(_ ++ _)
    }

    val maxNestingLevel: Int = {
      fields.map(maxNestingLevelOf).max
    }

    private def maxNestingLevelOf(field: Field): Int = {
      field match {
        case _: Field.Leaf => 1
        case Field.Branch(_, children) => children.map(maxNestingLevelOf).max + 1
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

      // The map is incrementally updated while walking the schema tree from top down.
      val widthMap: mutable.Map[Int, Int] = mutable.Map()

      val dataWidths = schema.leafs.indices
        .map { i =>
          data.map(_(i).length).max
        }
        .map(_ + 2)

      schema.leafs.zipWithIndex.foreach {
        case (leaf, i) =>
          val dataWidth = dataWidths(i)
          widthMap += (leaf.id() -> (dataWidth max (leaf.name.length + 2)))
      }

      schema.fields.foreach { root =>
        def updateWidth(field: Field, lowerBound: Int): Unit = {
          field match {
            case branch @ Field.Branch(name, children) =>
              val leafLowerBound =
                Math
                  .ceil((lowerBound max name.length + 2).toDouble / branch.leafs.size.toDouble)
                  .toInt
              children.foreach(child => updateWidth(child, leafLowerBound * child.leafs.size))
              val childrenWidth =
                children.map(child => widthMap(child.id())).sum
              val width = childrenWidth + children.size - 1
              val hash = branch.id()
              widthMap += hash -> width
            case leaf @ Field.Leaf(name) =>
              val hash = leaf.id()
              val newWidth = widthMap(hash) max lowerBound
              widthMap.put(hash, newWidth)
            case _ => new IllegalStateException()
          }
        }

        updateWidth(root, 0)
      }

      trait SchemaCell
      case class Given(field: Field) extends SchemaCell
      case class PlaceHolder(leaf: Field.Leaf) extends SchemaCell

      (0 until schema.maxNestingLevel).foldRight[Seq[SchemaCell]](schema.fields.map(Given)) {
        case (_, cells) =>
          val schemaLine = cells
            .map {
              case Given(field) =>
                (field.name, widthMap(field.id()))
              case PlaceHolder(leaf) =>
                ("", widthMap(leaf.id()))
            }
            .map {
              case (name, width) =>
                StringUtils.center(name, width)
            }
            .mkString("|", "|", "|")
          printer.println(schemaLine)
          cells.flatMap { f =>
            f match {
              case Given(Field.Branch(name, children)) => children.map(Given)
              case Given(l @ Field.Leaf(name)) => List(PlaceHolder(l))
              case p: PlaceHolder => List(p)
              case _ => throw new IllegalStateException()
            }
          }
      }

      val separationLine = schema.leafs
        .map { leaf =>
          widthMap(leaf.id())
        }
        .map { width =>
          new String(Array.tabulate(width)(_ => '-'))
        }
        .mkString("|", "|", "|")

      printer.println(separationLine)

      data.foreach { row =>
        val dataLine = row
          .zip(schema.leafs)
          .map {
            case (value, leaf) =>
              (value, widthMap(leaf.id()))
          }
          .map {
            case (value, width) =>
              StringUtils.leftPad(value, width)
          }
          .mkString("|", "|", "|")
        printer.println(dataLine)
      }

      printer.flush()
    }

    override def toString: String = {
      val out = new ByteArrayOutputStream()
      print(out)
      out.toString
    }
  }

  trait RowParser[ROW <: Any] {
    def parse(rowFactory: RowAppender, row: ROW): Unit
  }

  object RowParser {
    implicit object StringSeqParser extends RowParser[Seq[String]] {
      override def parse(rowFactory: RowAppender, row: Seq[String]): Unit = {
        val inc = rowFactory.incremental()
        row.foreach(ceil => inc.next().write(ceil))
      }
    }

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
          private val mutableRow = Array.tabulate(schema.leafs.size) { _ =>
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
                val out = new FieldAppenderImpl(schema.leafs(offset), mutableRow, offset)
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
            case Field.Branch(_, children) =>
              assert(children.count(_.name == name) == 1)
              val child = children.zipWithIndex.find(_._1.name == name).getOrElse {
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
          mutableRow(column) = toString(value)
        }

        private def toString(value: Any): String = value match {
          case Some(v) => toString(v)
          case None => "N/A"
          case other => other.toString
        }
      }
    }
  }
}
