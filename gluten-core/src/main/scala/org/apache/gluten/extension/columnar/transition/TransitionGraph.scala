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

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.SparkReflectionUtil

object TransitionGraph {
  trait Vertex {
    override def toString: String = SparkReflectionUtil.getSimpleClassName(this.getClass)
  }

  type Builder = FloydWarshallGraph.Builder[TransitionGraph.Vertex, Transition]

  def builder(): Builder = {
    FloydWarshallGraph.builder(TransitionCostModel)
  }

  implicit class TransitionGraphOps(val graph: TransitionGraph) {
    import TransitionGraphOps._
    def hasTransition(from: TransitionGraph.Vertex, to: TransitionGraph.Vertex): Boolean = {
      graph.hasPath(from, to)
    }

    def transitionOf(from: TransitionGraph.Vertex, to: TransitionGraph.Vertex): Transition = {
      val path = graph.pathOf(from, to)
      val out = path.edges().reduceOption((l, r) => chain(l, r)).getOrElse(Transition.empty)
      out
    }

    def transitionOfOption(
        from: TransitionGraph.Vertex,
        to: TransitionGraph.Vertex): Option[Transition] = {
      if (!hasTransition(from, to)) {
        return None
      }
      Some(transitionOf(from, to))
    }
  }

  private case class ChainedTransition(first: Transition, second: Transition) extends Transition {
    override def apply0(plan: SparkPlan): SparkPlan = {
      second(first(plan))
    }
  }

  private object TransitionGraphOps {
    private def chain(first: Transition, second: Transition): Transition = {
      if (first.isEmpty && second.isEmpty) {
        return Transition.empty
      }
      ChainedTransition(first, second)
    }
  }

  private case class TransitionCost(count: Int) extends FloydWarshallGraph.Cost {
    override def +(other: FloydWarshallGraph.Cost): TransitionCost = {
      other match {
        case TransitionCost(otherCount) => TransitionCost(count + otherCount)
      }
    }
  }

  private object TransitionCostModel extends FloydWarshallGraph.CostModel[Transition] {
    override def zero(): FloydWarshallGraph.Cost = TransitionCost(0)
    override def costOf(transition: Transition): FloydWarshallGraph.Cost = costOf0(transition)
    override def costComparator(): Ordering[FloydWarshallGraph.Cost] = Ordering.Int.on {
      case TransitionCost(c) => c
    }
    private def costOf0(transition: Transition): TransitionCost = transition match {
      case t if t.isEmpty => TransitionCost(0)
      case ChainedTransition(f, s) => costOf0(f) + costOf0(s)
      case _ => TransitionCost(0)
    }
  }
}
