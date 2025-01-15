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
package org.apache.gluten.extension.columnar.transition

import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.atomic.AtomicInteger

class FloydWarshallGraphSuite extends AnyFunSuite {
  import FloydWarshallGraphSuite._
  test("Sanity") {
    val v0 = Vertex()
    val v1 = Vertex()
    val v2 = Vertex()
    val v3 = Vertex()
    val v4 = Vertex()

    val e01 = Edge(5)
    val e12 = Edge(6)
    val e03 = Edge(2)
    val e34 = Edge(1)
    val e42 = Edge(3)

    val graph = FloydWarshallGraph
      .builder()
      .addVertex(v0)
      .addVertex(v1)
      .addVertex(v2)
      .addVertex(v3)
      .addVertex(v4)
      .addEdge(v0, v1, e01)
      .addEdge(v1, v2, e12)
      .addEdge(v0, v3, e03)
      .addEdge(v3, v4, e34)
      .addEdge(v4, v2, e42)
      .build(CostModel)

    assert(graph.hasPath(v0, v1))
    assert(graph.hasPath(v0, v2))
    assert(!graph.hasPath(v1, v0))
    assert(!graph.hasPath(v2, v0))

    assert(graph.pathOf(v0, v0).edges() == Nil)

    assert(graph.pathOf(v0, v1).edges() == Seq(e01))
    assert(graph.pathOf(v1, v2).edges() == Seq(e12))
    assert(graph.pathOf(v0, v3).edges() == Seq(e03))
    assert(graph.pathOf(v3, v4).edges() == Seq(e34))
    assert(graph.pathOf(v4, v2).edges() == Seq(e42))

    assert(graph.pathOf(v0, v2).edges() == Seq(e03, e34, e42))
  }
}

private object FloydWarshallGraphSuite {
  case class Vertex private (id: Int)

  private object Vertex {
    private val id = new AtomicInteger(0)

    def apply(): Vertex = {
      Vertex(id.getAndIncrement())
    }
  }

  case class Edge private (id: Int, distance: Long)

  private object Edge {
    private val id = new AtomicInteger(0)

    def apply(distance: Long): Edge = {
      Edge(id.getAndIncrement(), distance)
    }
  }

  private case class LongCost(c: Long) extends FloydWarshallGraph.Cost

  private object CostModel extends FloydWarshallGraph.CostModel[Edge] {
    override def zero(): FloydWarshallGraph.Cost = LongCost(0)
    override def sum(
        one: FloydWarshallGraph.Cost,
        other: FloydWarshallGraph.Cost): FloydWarshallGraph.Cost = {
      LongCost(one.asInstanceOf[LongCost].c + other.asInstanceOf[LongCost].c)
    }
    override def costOf(edge: Edge): FloydWarshallGraph.Cost = LongCost(edge.distance * 10)
    override def costComparator(): Ordering[FloydWarshallGraph.Cost] = Ordering.Long.on {
      case LongCost(c) => c
    }
  }
}
