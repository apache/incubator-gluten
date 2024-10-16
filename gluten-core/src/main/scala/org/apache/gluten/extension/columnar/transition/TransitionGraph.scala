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

  private case class TransitionCost(count: Int, nodeNames: Seq[String])
    extends FloydWarshallGraph.Cost {
    override def +(other: FloydWarshallGraph.Cost): TransitionCost = {
      other match {
        case TransitionCost(otherCount, otherNodeNames) =>
          TransitionCost(count + otherCount, nodeNames ++ otherNodeNames)
      }
    }
  }

  private object TransitionCostModel extends FloydWarshallGraph.CostModel[Transition] {
    override def zero(): TransitionCost = TransitionCost(0, Nil)
    override def costOf(transition: Transition): TransitionCost = {
      costOf0(transition)
    }
    override def costComparator(): Ordering[FloydWarshallGraph.Cost] = {
      (x: FloydWarshallGraph.Cost, y: FloydWarshallGraph.Cost) =>
        (x, y) match {
          case (TransitionCost(count, nodeNames), TransitionCost(otherCount, otherNodeNames)) =>
            if (count != otherCount) {
              count - otherCount
            } else {
              // To make the output order stable.
              nodeNames.mkString.hashCode - otherNodeNames.mkString.hashCode
            }
        }
    }

    private def costOf0(transition: Transition): TransitionCost = {
      val leaf = DummySparkPlan()

      /**
       * The calculation considers C2C's cost as half of C2R / R2C's cost. So query planner prefers
       * C2C than C2R / R2C.
       */
      def costOfPlan(plan: SparkPlan): TransitionCost = plan
        .map {
          case p if p == leaf => TransitionCost(0, Nil)
          case node @ RowToColumnarLike(_) => TransitionCost(2, Seq(node.nodeName))
          case node @ ColumnarToRowLike(_) => TransitionCost(2, Seq(node.nodeName))
          case node @ ColumnarToColumnarLike(_) => TransitionCost(1, Seq(node.nodeName))
        }
        .reduce((l, r) => l + r)

      val plan = transition.apply(leaf)
      val cost = costOfPlan(plan)
      cost
    }
  }
}
