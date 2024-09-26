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

type TransitionGraph = FloydWarshallGraph[Transition.Vertex, Transition]
type TransitionGraphBuilder = FloydWarshallGraph.Builder[Transition.Vertex, Transition]

object TransitionGraph {
  def builder(): TransitionGraphBuilder = {
    FloydWarshallGraph.builder(TransitionCostModel)
  }

  private case class TransitionCost(count: Int) extends FloydWarshallGraph.Cost {
    override def :+(other: FloydWarshallGraph.Cost): FloydWarshallGraph.Cost = {
      other match {
        case TransitionCost(otherCount) => TransitionCost(count + otherCount)
      }
    }
  }

  private object TransitionCostModel extends FloydWarshallGraph.CostModel[Transition] {
    override def zero(): FloydWarshallGraph.Cost = TransitionCost(0)
    override def costOf(edge: Transition): FloydWarshallGraph.Cost = TransitionCost(1)
    override def costComparator(): Ordering[FloydWarshallGraph.Cost] = Ordering.Int.on {
      case TransitionCost(c) => c
    }
  }
}
