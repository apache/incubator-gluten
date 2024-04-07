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

import org.apache.gluten.ras.RasCluster.ImmutableRasCluster
import org.apache.gluten.ras._
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.ras.vis.GraphvizVisualizer

import scala.collection.mutable

trait MemoLike[T <: AnyRef] {
  def memorize(node: T, constraintSet: PropertySet[T]): RasGroup[T]
}

trait Closure[T <: AnyRef] {
  def openFor(node: CanonicalNode[T]): MemoLike[T]
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

    private def toCacheKeyUnsafe(node: T): T = {
      val childrenClusters: Seq[RasClusterKey] = ras.planModel
        .childrenOf(node)
        .map(clusterOf)

      val newNode =
        ras.withNewChildren(
          node,
          childrenClusters.map(c => memoTable.groupOf(c, ras.propertySetFactory().any()).self()))

      newNode
    }

    private def toCacheKey(node: T): MemoCacheKey[T] = {
      MemoCacheKey(ras, toCacheKeyUnsafe(node))
    }

    private def clusterOf(n: T): RasClusterKey = {
      if (ras.planModel.isGroupLeaf(n)) {
        val plainGroup = memoTable.allGroups()(ras.planModel.getGroupId(n))
        return plainGroup.clusterKey()
      }

      val memoCacheKey = toCacheKey(n)

      if (cache.contains(memoCacheKey)) {
        cache(memoCacheKey)
      } else {
        // Node not yet added to cluster.
        val meta = ras.metadataModel.metadataOf(n)
        newCluster(meta)
      }
    }

    private def insertUnsafe(
        node: T,
        constraintSet: PropertySet[T]): RasGroup[T] = {

      val childrenGroups: Seq[RasGroup[T]] = ras.planModel
        .childrenOf(node)
        .zip(ras.propertySetFactory().childrenConstraintSets(constraintSet, node))
        .map {
          case (child, childConstraintSet) =>
            val childGroup = insertUnsafe(child, childConstraintSet)
            childGroup
        }

      val nodeUnsafe = ras.withNewChildren(node, childrenGroups.map(group => group.self()))
      val can = CanonicalNode(ras, nodeUnsafe)
      val cKey = can.toMemoCacheKey(memoTable)

      assert(cache.contains(cKey))
      val cluster = cache(cKey)
      addToCluster(cluster, can)

      val group = memoTable.groupOf(cluster, constraintSet)
      group
    }

    override def memorize(node: T, constraintSet: PropertySet[T]): RasGroup[T] = {
      clusterOf(node)
      insertUnsafe(node, constraintSet)
    }

    override def openFor(node: CanonicalNode[T]): MemoLike[T] = {
      val cacheKey = node.toMemoCacheKey(memoTable)
      assert(cache.contains(cacheKey))
      val targetCluster = cache(cacheKey)
      new InCusterMemo[T](this, targetCluster)
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
    private class InCusterMemo[T <: AnyRef](parent: RasMemo[T], preparedCluster: RasClusterKey)
      extends MemoLike[T] {

      private def prepare(node: T): Unit = {
        val cacheKey = parent.toCacheKey(node)
        if (!parent.cache.contains(cacheKey)) {
          return
        }
        val cachedCluster = parent.cache(cacheKey)
        if (cachedCluster == preparedCluster) {
          // The new node already memorized to memo and in the prepared cluster.
          return
        }
        // The new node already memorized to memo, but in the different cluster.
        // Merge the two clusters.
        //
        // TODO: Traverse up the tree to do more merges.
        parent.memoTable.mergeClusters(cachedCluster, preparedCluster)
      }

      override def memorize(node: T, constraintSet: PropertySet[T]): RasGroup[T] = {
        prepare(node)
        parent.insertUnsafe(node, constraintSet)
      }
    }
  }

  private object MemoCacheKey {
    def apply[T <: AnyRef](ras: Ras[T], self: T): MemoCacheKey[T] = {
      assert(ras.isCanonical(self))
      new MemoCacheKey[T](ras, self)
    }
  }

  private class MemoCacheKey[T <: AnyRef] private (ras: Ras[T], val self: T) {
    override def hashCode(): Int = ras.planModel.hashCode(self)
    override def equals(other: Any): Boolean = {
      other match {
        case that: MemoCacheKey[T] => ras.planModel.equals(self, that.self)
        case _ => false
      }
    }
  }

  implicit private class CanonicalNodeImplicits[T <: AnyRef](can: CanonicalNode[T]) {
    def toMemoCacheKey(memoTable: MemoTable.Writable[T]): MemoCacheKey[T] = {
      val ras = can.ras()
      val withDummyChildren = ras.withNewChildren(
        can.self(),
        can
          .getChildrenGroupIds()
          .map(gid => memoTable.allGroups()(gid))
          .map(g => memoTable.groupOf(g.clusterKey(), ras.propertySetFactory().any()).self())
      )
      MemoCacheKey(ras, withDummyChildren)
    }
  }
}

trait MemoStore[T <: AnyRef] {
  def getCluster(key: RasClusterKey): RasCluster[T]
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
  def allClusters(): Iterable[RasCluster[T]]
  def allGroups(): Seq[RasGroup[T]]
}

object MemoState {
  def apply[T <: AnyRef](
      ras: Ras[T],
      clusterLookup: Map[RasClusterKey, ImmutableRasCluster[T]],
      allGroups: Seq[RasGroup[T]]): MemoState[T] = {
    MemoStateImpl(ras, clusterLookup, allGroups)
  }

  private case class MemoStateImpl[T <: AnyRef](
      override val ras: Ras[T],
      override val clusterLookup: Map[RasClusterKey, ImmutableRasCluster[T]],
      override val allGroups: Seq[RasGroup[T]])
    extends MemoState[T] {
    private val allClustersCopy = clusterLookup.values

    override def getCluster(key: RasClusterKey): RasCluster[T] = clusterLookup(key)
    override def getGroup(id: Int): RasGroup[T] = allGroups(id)
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
