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

import org.apache.gluten.integration.action.TableRender.Field._
import org.apache.gluten.integration.action.TableRender.RowParser
import org.apache.gluten.integration.action.TableRender.RowParser.FieldAppender

// The tests are manually run.
object TableRenderTest {
  def case0(): Unit = {
    val render: TableRender[Seq[String]] = TableRender.create(
      Branch("ABC", List(Branch("AB", List(Leaf("A"), Leaf("B"))), Leaf("C"))),
      Branch("DE", List(Leaf("D"), Leaf("E"))))(new RowParser[Seq[String]] {
      override def parse(rowFactory: FieldAppender.RowAppender, row: Seq[String]): Unit = {
        val inc = rowFactory.incremental()
        row.foreach(ceil => inc.next().write(ceil))
      }
    })
    render.print(Console.out)
    Console.out.println()
  }

  def case1(): Unit = {
    val render: TableRender[Seq[String]] = TableRender.create(
      Branch("ABC", List(Branch("AB", List(Leaf("A"), Leaf("B"))), Leaf("C"))),
      Branch("DE", List(Leaf("D"), Leaf("E"))))(new RowParser[Seq[String]] {
      override def parse(rowFactory: FieldAppender.RowAppender, row: Seq[String]): Unit = {
        val inc = rowFactory.incremental()
        row.foreach(ceil => inc.next().write(ceil))
      }
    })

    render.appendRow(List("aaaa", "b", "cccccc", "d", "eeeee"))
    render.print(Console.out)
    Console.out.println()
  }

  def case2(): Unit = {
    val render: TableRender[Seq[String]] = TableRender.create(
      Branch("ABC", List(Branch("AAAAAAAAABBBBBB", List(Leaf("A"), Leaf("B"))), Leaf("C"))),
      Branch("DE", List(Leaf("D"), Leaf("E"))))(new RowParser[Seq[String]] {
      override def parse(rowFactory: FieldAppender.RowAppender, row: Seq[String]): Unit = {
        val inc = rowFactory.incremental()
        row.foreach(ceil => inc.next().write(ceil))
      }
    })

    render.appendRow(List("aaaa", "b", "cccccc", "d", "eeeee"))
    render.print(Console.out)
    Console.out.println()
  }

  def case3(): Unit = {
    val render: TableRender[Seq[String]] = TableRender.create(
      Branch("ABC", List(Branch("AB", List(Leaf("A"), Leaf("B"))), Leaf("CCCCCCCCCCCCC"))),
      Branch("DE", List(Leaf("D"), Leaf("E"))))(new RowParser[Seq[String]] {
      override def parse(rowFactory: FieldAppender.RowAppender, row: Seq[String]): Unit = {
        val inc = rowFactory.incremental()
        row.foreach(ceil => inc.next().write(ceil))
      }
    })

    render.appendRow(List("aaaa", "b", "cccccc", "d", "eeeee"))
    render.appendRow(List("aaaaaaaaaaaaa", "b", "cccccc", "ddddddddddd", "eeeee"))
    render.print(Console.out)
    Console.out.println()
  }

  def main(args: Array[String]): Unit = {
    case0()
    case1()
    case2()
    case3()
  }
}
