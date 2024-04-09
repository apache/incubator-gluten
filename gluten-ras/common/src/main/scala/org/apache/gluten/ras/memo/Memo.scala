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
package org.apache.gluten.ras.memo

import org.apache.gluten.ras._
import org.apache.gluten.ras.Ras.UnsafeKey
import org.apache.gluten.ras.RasCluster.ImmutableRasCluster
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.ras.vis.GraphvizVisualizer

import scala.collection.mutable

trait MemoLike[T <: AnyRef] {
  def memorize(node: T, constraintSet: PropertySet[T]): RasGroup[T]
}

trait Closure[T <: AnyRef] {
  def openFor(cKey: RasClusterKey): MemoLike[T]
}

trait Memo[T <: AnyRef] extends Closure[T] with MemoLike[T] {
  def newState(): MemoState[T]
  def doExhaustively(func: => Unit): Unit
}

trait UnsafeMemo[T <: AnyRef] extends Memo[T] {
  def table(): MemoTable[T]
}

object Memo {
  def apply[T <: AnyRef](ras: Ras[T]): Memo[T] = {
    new RasMemo[T](ras)
  }

  def unsafe[T <: AnyRef](ras: Ras[T]): UnsafeMemo[T] = {
    new RasMemo[T](ras)
  }

  private class RasMemo[T <: AnyRef](val ras: Ras[T]) extends UnsafeMemo[T] {
    import RasMemo._
    private val memoTable: MemoTable.Writable[T] = MemoTable.create(ras)
    private val cache = mutable.Map[MemoCacheKey[T], RasClusterKey]()

    private def newCluster(metadata: Metadata): RasClusterKey = {
      memoTable.newCluster(metadata)
    }

    private def addToCluster(clusterKey: RasClusterKey, can: CanonicalNode[T]): Unit = {
      memoTable.addToCluster(clusterKey, can)
    }

    private def clusterOfUnsafe(metadata: Metadata, cacheKey: MemoCacheKey[T]): RasClusterKey = {
      if (cache.contains(cacheKey)) {
        cache(cacheKey)
      } else {
        // Node not yet added to cluster.
        val cluster = newCluster(metadata)
        cache += (cacheKey -> cluster)
        cluster
      }
    }

    private def dummyGroupOf(clusterKey: RasClusterKey): RasGroup[T] = {
      memoTable.getDummyGroup(clusterKey)
    }

    private def toCacheKeyUnsafe(n: T): MemoCacheKey[T] = {
      MemoCacheKey(ras, n)
    }

    private def prepareInsert(n: T): Prepare[T] = {
      if (ras.isGroupLeaf(n)) {
        val group = memoTable.getGroup(ras.planModel.getGroupId(n))
        return Prepare.cluster(this, group.clusterKey())
      }

      val childrenPrepares = ras.planModel.childrenOf(n).map(child => prepareInsert(child))

      val canUnsafe = ras.withNewChildren(
        n,
        childrenPrepares.map(childPrepare => dummyGroupOf(childPrepare.clusterKey()).self()))

      val cacheKey = toCacheKeyUnsafe(canUnsafe)

      val clusterKey = clusterOfUnsafe(ras.metadataModel.metadataOf(n), cacheKey)

      Prepare.tree(this, clusterKey, childrenPrepares)
    }

    override def memorize(node: T, constraintSet: PropertySet[T]): RasGroup[T] = {
      val prepare = prepareInsert(node)
      prepare.doInsert(node, constraintSet)
    }

    override def openFor(cKey: RasClusterKey): MemoLike[T] = {
      new InCusterMemo[T](this, cKey)
    }

    override def newState(): MemoState[T] = {
      memoTable.newState()
    }

    override def table(): MemoTable[T] = memoTable

    override def doExhaustively(func: => Unit): Unit = {
      memoTable.doExhaustively(func)
    }
  }

  private object RasMemo {
    private class InCusterMemo[T <: AnyRef](parent: RasMemo[T], targetCluster: RasClusterKey)
      extends MemoLike[T] {
      private val ras = parent.ras

      // TODO: Traverse up the tree to do more merges.
      private def prepareInsert(node: T): Prepare[T] = {
        if (ras.isGroupLeaf(node)) {
          val group = parent.memoTable.getGroup(ras.planModel.getGroupId(node))
          val residentCluster = group.clusterKey()

          if (residentCluster == targetCluster) {
            return Prepare.cluster(parent, targetCluster)
          }
          // The resident cluster of group leaf is not the same with target cluster.
          // Merge.
          parent.memoTable.mergeClusters(residentCluster, targetCluster)
          return Prepare.cluster(parent, targetCluster)
        }

        val childrenPrepares =
          ras.planModel.childrenOf(node).map(child => parent.prepareInsert(child))

        val canUnsafe = ras.withNewChildren(
          node,
          childrenPrepares.map {
            childPrepare => parent.dummyGroupOf(childPrepare.clusterKey()).self()
          })

        val cacheKey = parent.toCacheKeyUnsafe(canUnsafe)

        if (!parent.cache.contains(cacheKey)) {
          // The new node was not added to memo yet. Add it to the target cluster.
          parent.cache += (cacheKey -> targetCluster)
          return Prepare.tree(parent, targetCluster, childrenPrepares)
        }

        // The new node already memorized to memo.

        val cachedCluster = parent.cache(cacheKey)
        if (cachedCluster == targetCluster) {
          // The new node already memorized to memo and in the target cluster.
          return Prepare.tree(parent, targetCluster, childrenPrepares)
        }
        // The new node already memorized to memo, but in the different cluster.
        // Merge the two clusters.
        parent.memoTable.mergeClusters(cachedCluster, targetCluster)
        Prepare.tree(parent, targetCluster, childrenPrepares)
      }

      override def memorize(node: T, constraintSet: PropertySet[T]): RasGroup[T] = {
        val prepare = prepareInsert(node)
        prepare.doInsert(node, constraintSet)
      }
    }

    private trait Prepare[T <: AnyRef] {
      def clusterKey(): RasClusterKey
      def doInsert(node: T, constraintSet: PropertySet[T]): RasGroup[T]
    }

    private object Prepare {
      def tree[T <: AnyRef](
          memo: RasMemo[T],
          cKey: RasClusterKey,
          children: Seq[Prepare[T]]): Prepare[T] = {
        new TreePrepare[T](memo, cKey, children)
      }

      def cluster[T <: AnyRef](memo: RasMemo[T], cKey: RasClusterKey): Prepare[T] = {
        new ClusterPrepare[T](memo, cKey)
      }

      private class TreePrepare[T <: AnyRef](
          memo: RasMemo[T],
          override val clusterKey: RasClusterKey,
          children: Seq[Prepare[T]])
        extends Prepare[T] {
        private val ras = memo.ras

        override def doInsert(node: T, constraintSet: PropertySet[T]): RasGroup[T] = {
          assert(!ras.isGroupLeaf(node))
          val childrenGroups = children
            .zip(ras.planModel.childrenOf(node))
            .zip(ras.propertySetFactory().childrenConstraintSets(constraintSet, node))
            .map {
              case ((childPrepare, child), childConstraintSet) =>
                childPrepare.doInsert(child, childConstraintSet)
            }

          val canUnsafe = ras.withNewChildren(node, childrenGroups.map(group => group.self()))
          val can = CanonicalNode(ras, canUnsafe)

          memo.addToCluster(clusterKey, can)

          val group = memo.memoTable.groupOf(clusterKey, constraintSet)
          group
        }
      }

      private class ClusterPrepare[T <: AnyRef](memo: RasMemo[T], cKey: RasClusterKey)
        extends Prepare[T] {
        private val ras = memo.ras
        override def doInsert(node: T, constraintSet: PropertySet[T]): RasGroup[T] = {
          assert(ras.isGroupLeaf(node))
          memo.memoTable.groupOf(cKey, constraintSet)
        }

        override def clusterKey(): RasClusterKey = cKey
      }
    }
  }

  private object MemoCacheKey {
    def apply[T <: AnyRef](ras: Ras[T], self: T): MemoCacheKey[T] = {
      assert(ras.isCanonical(self))
      MemoCacheKey[T](ras.toUnsafeKey(self))
    }
  }

  private case class MemoCacheKey[T <: AnyRef] private (delegate: UnsafeKey[T])
}

trait MemoStore[T <: AnyRef] {
  def getCluster(key: RasClusterKey): RasCluster[T]
  def getDummyGroup(key: RasClusterKey): RasGroup[T]
  def getGroup(id: Int): RasGroup[T]
}

object MemoStore {
  implicit class MemoStoreImplicits[T <: AnyRef](store: MemoStore[T]) {
    def asGroupSupplier(): Int => RasGroup[T] = {
      store.getGroup
    }
  }
}

trait MemoState[T <: AnyRef] extends MemoStore[T] {
  def ras(): Ras[T]
  def clusterLookup(): Map[RasClusterKey, RasCluster[T]]
  def clusterDummyGroupLookup(): Map[RasClusterKey, RasGroup[T]]
  def allClusters(): Iterable[RasCluster[T]]
  def allGroups(): Seq[RasGroup[T]]
}

object MemoState {
  def apply[T <: AnyRef](
      ras: Ras[T],
      clusterLookup: Map[RasClusterKey, ImmutableRasCluster[T]],
      clusterDummyGroupLookup: Map[RasClusterKey, RasGroup[T]],
      allGroups: Seq[RasGroup[T]],
      allDummyGroups: Seq[RasGroup[T]]): MemoState[T] = {
    MemoStateImpl(ras, clusterLookup, clusterDummyGroupLookup, allGroups, allDummyGroups)
  }

  private case class MemoStateImpl[T <: AnyRef](
      override val ras: Ras[T],
      override val clusterLookup: Map[RasClusterKey, ImmutableRasCluster[T]],
      override val clusterDummyGroupLookup: Map[RasClusterKey, RasGroup[T]],
      override val allGroups: Seq[RasGroup[T]],
      allDummyGroups: Seq[RasGroup[T]])
    extends MemoState[T] {
    private val allClustersCopy = clusterLookup.values

    override def getCluster(key: RasClusterKey): RasCluster[T] = clusterLookup(key)
    override def getDummyGroup(key: RasClusterKey): RasGroup[T] = clusterDummyGroupLookup(key)
    override def getGroup(id: Int): RasGroup[T] = {
      if (id < 0) {
        val out = allDummyGroups((-id - 1))
        assert(out.id() == id)
        return out
      }
      allGroups(id)
    }
    override def allClusters(): Iterable[RasCluster[T]] = allClustersCopy
  }

  implicit class MemoStateImplicits[T <: AnyRef](state: MemoState[T]) {

    def formatGraphvizWithBest(best: Best[T]): String = {
      GraphvizVisualizer(state.ras(), state, best).format()
    }

    def formatGraphvizWithoutBest(rootGroupId: Int): String = {
      GraphvizVisualizer(state.ras(), state, rootGroupId).format()
    }
  }
}
