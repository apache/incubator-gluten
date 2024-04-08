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
package org.apache.gluten.ras.dp

import org.apache.gluten.ras._
import org.apache.gluten.ras.Best.KnownCostPath
import org.apache.gluten.ras.best.BestFinder
import org.apache.gluten.ras.dp.DpZipperAlgo.Adjustment.Panel
import org.apache.gluten.ras.memo.{Memo, MemoTable}
import org.apache.gluten.ras.path.{InClusterPath, PathFinder, RasPath}
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.ras.rule.{EnforcerRuleSet, RuleApplier, Shape}

// TODO: Branch and bound pruning.
private class DpPlanner[T <: AnyRef] private (
    ras: Ras[T],
    altConstraintSets: Seq[PropertySet[T]],
    constraintSet: PropertySet[T],
    plan: T)
  extends RasPlanner[T] {
  import DpPlanner._

  private val memo = Memo.unsafe(ras)
  private val rules = ras.ruleFactory.create().map(rule => RuleApplier(ras, memo, rule))
  private val enforcerRuleSet = EnforcerRuleSet[T](ras, memo)

  private lazy val rootGroupId: Int = {
    memo.memorize(plan, constraintSet).id()
  }

  private lazy val best: (Best[T], KnownCostPath[T]) = {
    altConstraintSets.foreach(propSet => memo.memorize(plan, propSet))
    val groupId = rootGroupId
    val memoTable = memo.table()
    val best = findBest(memoTable, groupId)
    (best, best.path())
  }

  override def plan(): T = {
    best._2.rasPath.plan()
  }

  override def newState(): PlannerState[T] = {
    val foundBest = best._1
    PlannerState(ras, memo.newState(), rootGroupId, foundBest)
  }

  private def findBest(memoTable: MemoTable[T], groupId: Int): Best[T] = {
    val cKey = memoTable.allGroups()(groupId).clusterKey()
    val algoDef = new DpExploreAlgoDef[T]
    val adjustment = new ExploreAdjustment(ras, memoTable, rules, enforcerRuleSet)
    DpClusterAlgo.resolve(memoTable, algoDef, adjustment, cKey)
    val finder = BestFinder(ras, memoTable.newState())
    finder.bestOf(groupId)
  }
}

object DpPlanner {
  def apply[T <: AnyRef](
      ras: Ras[T],
      altConstraintSets: Seq[PropertySet[T]],
      constraintSet: PropertySet[T],
      plan: T): RasPlanner[T] = {
    new DpPlanner(ras, altConstraintSets: Seq[PropertySet[T]], constraintSet, plan)
  }

  // Visited flag.
  sealed private trait SolvedFlag
  private case object Solved extends SolvedFlag

  private class DpExploreAlgoDef[T <: AnyRef] extends DpClusterAlgoDef[T, SolvedFlag, SolvedFlag] {
    override def solveNode(
        node: InClusterNode[T],
        childrenClustersOutput: RasClusterKey => SolvedFlag): SolvedFlag = Solved
    override def solveCluster(
        group: RasClusterKey,
        nodesOutput: InClusterNode[T] => SolvedFlag): SolvedFlag = Solved
    override def solveNodeOnCycle(node: InClusterNode[T]): SolvedFlag = Solved
    override def solveClusterOnCycle(cluster: RasClusterKey): SolvedFlag = Solved
  }

  private class ExploreAdjustment[T <: AnyRef](
      ras: Ras[T],
      memoTable: MemoTable[T],
      rules: Seq[RuleApplier[T]],
      enforcerRuleSet: EnforcerRuleSet[T])
    extends DpClusterAlgo.Adjustment[T] {

    override def exploreChildX(
        panel: Panel[InClusterNode[T], RasClusterKey],
        x: InClusterNode[T]): Unit = {}
    override def exploreChildY(
        panel: Panel[InClusterNode[T], RasClusterKey],
        y: RasClusterKey): Unit = {}
    override def exploreParentX(
        panel: Panel[InClusterNode[T], RasClusterKey],
        x: InClusterNode[T]): Unit = {}

    override def exploreParentY(
        panel: Panel[InClusterNode[T], RasClusterKey],
        cKey: RasClusterKey): Unit = {
      memoTable.doExhaustively {
        applyEnforcerRules(panel, cKey)
        applyRules(panel, cKey)
      }
    }

    private def applyRules(
        panel: Panel[InClusterNode[T], RasClusterKey],
        cKey: RasClusterKey): Unit = {
      if (rules.isEmpty) {
        return
      }
      val dummyGroup = memoTable.getDummyGroup(cKey)
      val shapes = rules.map(_.shape())
      findPaths(GroupNode(ras, dummyGroup), shapes) {
        path => rules.foreach(rule => applyRule(panel, cKey, rule, path))
      }
    }

    private def applyEnforcerRules(
        panel: Panel[InClusterNode[T], RasClusterKey],
        cKey: RasClusterKey): Unit = {
      val dummyGroup = memoTable.getDummyGroup(cKey)
      cKey.propSets(memoTable).foreach {
        constraintSet =>
          val enforcerRules = enforcerRuleSet.rulesOf(constraintSet)
          if (enforcerRules.nonEmpty) {
            val shapes = enforcerRules.map(_.shape())
            findPaths(GroupNode(ras, dummyGroup), shapes) {
              path => enforcerRules.foreach(rule => applyRule(panel, cKey, rule, path))
            }
          }
      }
    }

    private def findPaths(gn: GroupNode[T], shapes: Seq[Shape[T]])(
        onFound: RasPath[T] => Unit): Unit = {
      val finder = shapes
        .foldLeft(
          PathFinder
            .builder(ras, memoTable)) {
          case (builder, shape) =>
            builder.output(shape.wizard())
        }
        .build()
      finder.find(gn).foreach(path => onFound(path))
    }

    private def applyRule(
        panel: Panel[InClusterNode[T], RasClusterKey],
        thisClusterKey: RasClusterKey,
        rule: RuleApplier[T],
        path: RasPath[T]): Unit = {
      val probe = memoTable.probe()
      rule.apply(InClusterPath(thisClusterKey, path))
      val diff = probe.toDiff()
      val changedClusters = diff.changedClusters()
      if (changedClusters.isEmpty) {
        return
      }

      // One or more cluster changed. If they're not the current cluster, we should
      // withdraw the DP results for them to trigger re-computation. Since
      // changed cluster (may created new groups, may added new nodes) could expand the
      // search spaces again.

      changedClusters.foreach {
        case cKey if cKey == thisClusterKey =>
        // This cluster has been changed. This cluster is being solved so we
        // don't have to invalidate.
        case cKey =>
          // Changes happened on another cluster. Invalidate solution for the cluster
          // To trigger re-computation.
          panel.invalidateYSolution(cKey)
      }
    }
  }

  private object ExploreAdjustment {}
}
