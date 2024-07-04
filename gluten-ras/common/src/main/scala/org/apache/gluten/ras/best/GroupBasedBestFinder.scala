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
package org.apache.gluten.ras.best

import org.apache.gluten.ras._
import org.apache.gluten.ras.Best.{BestNotFoundException, KnownCostPath}
import org.apache.gluten.ras.best.BestFinder.KnownCostGroup
import org.apache.gluten.ras.dp.{DpGroupAlgo, DpGroupAlgoDef}
import org.apache.gluten.ras.memo.MemoState
import org.apache.gluten.ras.path.{PathKeySet, RasPath}

import java.util

// The best path's each sub-path is considered optimal in its own group.
private class GroupBasedBestFinder[T <: AnyRef](
    ras: Ras[T],
    memoState: MemoState[T],
    adjustment: DpGroupAlgo.Adjustment[T])
  extends BestFinder[T] {
  import GroupBasedBestFinder._

  private val allGroups = memoState.allGroups()

  override def bestOf(groupId: Int): Best[T] = {
    val group = allGroups(groupId)
    val groupToCosts = fillBests(group)
    if (!groupToCosts.contains(groupId)) {
      throw BestNotFoundException(
        s"Best path not found. Memo state (Graphviz): \n" +
          s"${memoState.formatGraphvizWithoutBest(groupId)}")
    }
    BestFinder.newBest(ras, group, groupToCosts)
  }

  private def fillBests(group: RasGroup[T]): Map[Int, KnownCostGroup[T]] = {
    val algoDef = new AlgoDef(ras, memoState)
    val solution = DpGroupAlgo.resolve(memoState, algoDef, adjustment, group)
    val bests = allGroups.flatMap {
      group =>
        if (solution.isYSolved(group)) {
          solution.solutionOfY(group).flatMap(kcg => Some(group.id() -> kcg))
        } else {
          None
        }
    }.toMap
    bests
  }
}

private object GroupBasedBestFinder {
  private[best] def algoDef[T <: AnyRef](ras: Ras[T], memoState: MemoState[T])
      : DpGroupAlgoDef[T, Option[KnownCostPath[T]], Option[KnownCostGroup[T]]] = {
    new AlgoDef(ras, memoState)
  }

  private class AlgoDef[T <: AnyRef](ras: Ras[T], memoState: MemoState[T])
    extends DpGroupAlgoDef[T, Option[KnownCostPath[T]], Option[KnownCostGroup[T]]] {
    private val allGroups = memoState.allGroups()
    private val costComparator = ras.costModel.costComparator()

    override def solveNode(
        ign: InGroupNode[T],
        childrenGroupsOutput: RasGroup[T] => Option[KnownCostGroup[T]])
        : Option[KnownCostPath[T]] = {
      val can = ign.can
      if (can.isLeaf()) {
        val path = RasPath.one(ras, PathKeySet.trivial, allGroups, can)
        return Some(KnownCostPath(ras, path))
      }
      val childrenGroups = can.getChildrenGroups(allGroups).map(gn => allGroups(gn.groupId()))
      val maybeBestChildrenPaths: Seq[Option[KnownCostPath[T]]] = childrenGroups.map {
        childGroup => childrenGroupsOutput(childGroup).map(kcg => kcg.best())
      }
      if (maybeBestChildrenPaths.exists(_.isEmpty)) {
        // Node should only be solved when all children outputs exist.
        return None
      }
      val bestChildrenPaths = maybeBestChildrenPaths.map(_.get)
      val kcp = KnownCostPath(ras, path.RasPath(ras, can, bestChildrenPaths.map(_.rasPath)).get)
      // Cost should be in monotonically increasing basis.
      bestChildrenPaths.map(_.cost).foreach {
        childCost =>
          assert(
            ras.costModel.costComparator().gteq(kcp.cost, childCost),
            "Illegal decreasing cost")
      }
      Some(kcp)
    }

    override def solveGroup(
        group: RasGroup[T],
        nodesOutput: InGroupNode[T] => Option[KnownCostPath[T]]): Option[KnownCostGroup[T]] = {
      import scala.collection.JavaConverters._

      val nodes = group.nodes(memoState)
      // Allow unsolved children nodes while solving group.
      val flatNodesOutput = new util.IdentityHashMap[CanonicalNode[T], KnownCostPath[T]]()

      nodes
        .flatMap(n => nodesOutput(InGroupNode(group.id(), n)).map(kcp => n -> kcp))
        .foreach {
          case (n, kcp) =>
            assert(!flatNodesOutput.containsKey(n))
            flatNodesOutput.put(n, kcp)
        }

      if (flatNodesOutput.isEmpty) {
        return None
      }
      val bestPath = flatNodesOutput.values.asScala.reduce {
        (left, right) =>
          Ordering
            .by((cp: KnownCostPath[T]) => cp.cost)(costComparator)
            .min(left, right)
      }
      Some(
        KnownCostGroup(
          nodes,
          n => Option(flatNodesOutput.get(n)),
          bestPath.rasPath.node().self().asCanonical()))
    }

    override def solveNodeOnCycle(node: InGroupNode[T]): Option[KnownCostPath[T]] =
      None

    override def solveGroupOnCycle(cluster: RasGroup[T]): Option[KnownCostGroup[T]] = {
      None
    }
  }
}
