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
  def apply[T <: AnyRef](ras: Ras[T], constraintSet: PropertySet[T], plan: T): RasPlanner[T] = {
    ras.config.plannerType match {
      case PlannerType.Exhaustive =>
        ExhaustivePlanner(ras, constraintSet, plan)
      case PlannerType.Dp =>
        DpPlanner(ras, constraintSet, plan)
    }
  }
}

trait Best[T <: AnyRef] {
  import Best._
  def rootGroupId(): Int
  def bestNodes(): InGroupNode[T] => Boolean
  def winnerNodes(): InGroupNode[T] => Boolean
  def costs(): InGroupNode[T] => Option[Cost]
  def path(): KnownCostPath[T]
}

object Best {
  def apply[T <: AnyRef](
      ras: Ras[T],
      rootGroupId: Int,
      bestPath: KnownCostPath[T],
      winnerNodes: Seq[InGroupNode[T]],
      costs: InGroupNode[T] => Option[Cost]): Best[T] = {
    val bestNodes = mutable.Set[InGroupNode.UniqueKey]()

    def dfs(groupId: Int, cursor: RasPath.PathNode[T]): Unit = {
      val can = cursor.self().asCanonical()
      bestNodes += InGroupNode(groupId, can).toUniqueKey
      cursor.zipChildrenWithGroupIds().foreach {
        case (childPathNode, childGroupId) =>
          dfs(childGroupId, childPathNode)
      }
    }

    dfs(rootGroupId, bestPath.rasPath.node())

    val bestNodeSet = bestNodes.toSet
    val winnerNodeSet = winnerNodes.map(_.toUniqueKey).toSet

    BestImpl(
      ras,
      rootGroupId,
      bestPath,
      n => bestNodeSet.contains(n.toUniqueKey),
      n => winnerNodeSet.contains(n.toUniqueKey),
      costs)
  }

  private case class BestImpl[T <: AnyRef](
      ras: Ras[T],
      override val rootGroupId: Int,
      override val path: KnownCostPath[T],
      override val bestNodes: InGroupNode[T] => Boolean,
      override val winnerNodes: InGroupNode[T] => Boolean,
      override val costs: InGroupNode[T] => Option[Cost])
    extends Best[T]

  trait KnownCostPath[T <: AnyRef] {
    def rasPath: RasPath[T]
    def cost: Cost
  }

  object KnownCostPath {
    def apply[T <: AnyRef](ras: Ras[T], rasPath: RasPath[T]): KnownCostPath[T] = {
      KnownCostPathImpl(rasPath, ras.costModel.costOf(rasPath.plan()))
    }

    def apply[T <: AnyRef](rasPath: RasPath[T], cost: Cost): KnownCostPath[T] = {
      KnownCostPathImpl(rasPath, cost)
    }

    private case class KnownCostPathImpl[T <: AnyRef](rasPath: RasPath[T], cost: Cost)
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
  def ras(): Ras[T]
  def memoState(): MemoState[T]
  def rootGroupId(): Int
  def best(): Best[T]
}

object PlannerState {
  def apply[T <: AnyRef](
      ras: Ras[T],
      memoState: MemoState[T],
      rootGroupId: Int,
      best: Best[T]): PlannerState[T] = {
    PlannerStateImpl(ras, memoState, rootGroupId, best)
  }

  implicit class PlannerStateImplicits[T <: AnyRef](state: PlannerState[T]) {
    def formatGraphviz(): String = {
      formatGraphvizWithBest()
    }

    private def formatGraphvizWithBest(): String = {
      GraphvizVisualizer(state.ras(), state.memoState(), state.best()).format()
    }
  }

  private case class PlannerStateImpl[T <: AnyRef] private (
      override val ras: Ras[T],
      override val memoState: MemoState[T],
      override val rootGroupId: Int,
      override val best: Best[T])
    extends PlannerState[T]
}
