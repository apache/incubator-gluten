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
package org.apache.gluten.ras

import org.apache.gluten.ras.RasConfig.PlannerType
import org.apache.gluten.ras.dp.DpPlanner
import org.apache.gluten.ras.exaustive.ExhaustivePlanner
import org.apache.gluten.ras.memo.MemoState
import org.apache.gluten.ras.path.RasPath
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.ras.vis.GraphvizVisualizer

import scala.collection.mutable

trait RasPlanner[T <: AnyRef] {
  def plan(): T
  def newState(): PlannerState[T]
}

object RasPlanner {
  def apply[T <: AnyRef](
                          cbo: Ras[T],
                          altConstraintSets: Seq[PropertySet[T]],
                          constraintSet: PropertySet[T],
                          plan: T): RasPlanner[T] = {
    cbo.config.plannerType match {
      case PlannerType.Exhaustive =>
        ExhaustivePlanner(cbo, altConstraintSets, constraintSet, plan)
      case PlannerType.Dp =>
        DpPlanner(cbo, altConstraintSets, constraintSet, plan)
    }
  }
}

trait Best[T <: AnyRef] {
  import Best._
  def rootGroupId(): Int
  def bestNodes(): Set[InGroupNode[T]]
  def winnerNodes(): Set[InGroupNode[T]]
  def costs(): InGroupNode[T] => Option[Cost]
  def path(): KnownCostPath[T]
}

object Best {
  def apply[T <: AnyRef](
                          cbo: Ras[T],
                          rootGroupId: Int,
                          bestPath: KnownCostPath[T],
                          winnerNodes: Seq[InGroupNode[T]],
                          costs: InGroupNode[T] => Option[Cost]): Best[T] = {
    val bestNodes = mutable.Set[InGroupNode[T]]()

    def dfs(groupId: Int, cursor: RasPath.PathNode[T]): Unit = {
      val can = cursor.self().asCanonical()
      bestNodes += InGroupNode(groupId, can)
      cursor.zipChildrenWithGroupIds().foreach {
        case (childPathNode, childGroupId) =>
          dfs(childGroupId, childPathNode)
      }
    }

    dfs(rootGroupId, bestPath.cboPath.node())

    val winnerNodeSet = winnerNodes.toSet

    BestImpl(cbo, rootGroupId, bestPath, bestNodes.toSet, winnerNodeSet, costs)
  }

  private case class BestImpl[T <: AnyRef](
                                            cbo: Ras[T],
                                            override val rootGroupId: Int,
                                            override val path: KnownCostPath[T],
                                            override val bestNodes: Set[InGroupNode[T]],
                                            override val winnerNodes: Set[InGroupNode[T]],
                                            override val costs: InGroupNode[T] => Option[Cost])
    extends Best[T]

  trait KnownCostPath[T <: AnyRef] {
    def cboPath: RasPath[T]
    def cost: Cost
  }

  object KnownCostPath {
    def apply[T <: AnyRef](cbo: Ras[T], cboPath: RasPath[T]): KnownCostPath[T] = {
      KnownCostPathImpl(cboPath, cbo.costModel.costOf(cboPath.plan()))
    }

    def apply[T <: AnyRef](cboPath: RasPath[T], cost: Cost): KnownCostPath[T] = {
      KnownCostPathImpl(cboPath, cost)
    }

    private case class KnownCostPathImpl[T <: AnyRef](cboPath: RasPath[T], cost: Cost)
      extends KnownCostPath[T]
  }

  case class BestNotFoundException(message: String, cause: Exception)
    extends RuntimeException(message, cause)
  object BestNotFoundException {
    def apply(message: String): BestNotFoundException = {
      BestNotFoundException(message, null)
    }
    def apply(): BestNotFoundException = {
      BestNotFoundException(null, null)
    }
  }
}

trait PlannerState[T <: AnyRef] {
  def cbo(): Ras[T]
  def memoState(): MemoState[T]
  def rootGroupId(): Int
  def best(): Best[T]
}

object PlannerState {
  def apply[T <: AnyRef](
                          cbo: Ras[T],
                          memoState: MemoState[T],
                          rootGroupId: Int,
                          best: Best[T]): PlannerState[T] = {
    PlannerStateImpl(cbo, memoState, rootGroupId, best)
  }

  implicit class PlannerStateImplicits[T <: AnyRef](state: PlannerState[T]) {
    def formatGraphviz(): String = {
      formatGraphvizWithBest()
    }

    private def formatGraphvizWithBest(): String = {
      GraphvizVisualizer(state.cbo(), state.memoState(), state.best()).format()
    }
  }

  private case class PlannerStateImpl[T <: AnyRef] private (
                                                             override val cbo: Ras[T],
                                                             override val memoState: MemoState[T],
                                                             override val rootGroupId: Int,
                                                             override val best: Best[T])
    extends PlannerState[T]
}
