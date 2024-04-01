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
import org.apache.gluten.ras.RasCluster.ImmutableRasCluster
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.ras.util.CanonicalNodeMap
import org.apache.gluten.ras.vis.GraphvizVisualizer

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
  def apply[T <: AnyRef](cbo: Ras[T]): Memo[T] = {
    new RasMemo[T](cbo)
  }

  def unsafe[T <: AnyRef](cbo: Ras[T]): UnsafeMemo[T] = {
    new RasMemo[T](cbo)
  }

  private class RasMemo[T <: AnyRef](val cbo: Ras[T]) extends UnsafeMemo[T] {
    import RasMemo._
    private val memoTable: MemoTable.Writable[T] = MemoTable.create(cbo)
    private val cache: NodeToClusterMap[T] = new NodeToClusterMap(cbo)

    private def newCluster(metadata: Metadata): RasClusterKey = {
      memoTable.newCluster(metadata)
    }

    private def addToCluster(clusterKey: RasClusterKey, can: CanonicalNode[T]): Unit = {
      assert(!cache.contains(can))
      cache.put(can, clusterKey)
      memoTable.addToCluster(clusterKey, can)
    }

    // Replace node's children with node groups. When a group doesn't exist, create it.
    private def canonizeUnsafe(node: T, constraintSet: PropertySet[T], depth: Int): T = {
      assert(depth >= 1)
      if (depth > 1) {
        return cbo.withNewChildren(
          node,
          cbo.planModel
            .childrenOf(node)
            .zip(cbo.propertySetFactory().childrenConstraintSets(constraintSet, node))
            .map {
              case (child, constraintSet) =>
                canonizeUnsafe(child, constraintSet, depth - 1)
            }
        )
      }
      assert(depth == 1)
      val childrenGroups: Seq[RasGroup[T]] = cbo.planModel
        .childrenOf(node)
        .zip(cbo.propertySetFactory().childrenConstraintSets(constraintSet, node))
        .map {
          case (child, childConstraintSet) =>
            memorize(child, childConstraintSet)
        }
      val newNode =
        cbo.withNewChildren(node, childrenGroups.map(group => group.self()))
      newNode
    }

    private def canonize(node: T, constraintSet: PropertySet[T]): CanonicalNode[T] = {
      CanonicalNode(cbo, canonizeUnsafe(node, constraintSet, 1))
    }

    private def insert(n: T, constraintSet: PropertySet[T]): RasClusterKey = {
      if (cbo.planModel.isGroupLeaf(n)) {
        val plainGroup = memoTable.allGroups()(cbo.planModel.getGroupId(n))
        return plainGroup.clusterKey()
      }

      val node = canonize(n, constraintSet)

      if (cache.contains(node)) {
        cache.get(node)
      } else {
        // Node not yet added to cluster.
        val meta = cbo.metadataModel.metadataOf(node.self())
        val clusterKey = newCluster(meta)
        addToCluster(clusterKey, node)
        clusterKey
      }
    }

    override def memorize(node: T, constraintSet: PropertySet[T]): RasGroup[T] = {
      val clusterKey = insert(node, constraintSet)
      val prevGroupCount = memoTable.allGroups().size
      val out = memoTable.groupOf(clusterKey, constraintSet)
      val newGroupCount = memoTable.allGroups().size
      assert(newGroupCount >= prevGroupCount)
      out
    }

    override def openFor(node: CanonicalNode[T]): MemoLike[T] = {
      assert(cache.contains(node))
      val targetCluster = cache.get(node)
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

      private def insert(node: T, constraintSet: PropertySet[T]): Unit = {
        val can = parent.canonize(node, constraintSet)
        if (parent.cache.contains(can)) {
          val cachedCluster = parent.cache.get(can)
          if (cachedCluster == preparedCluster) {
            return
          }
          // The new node already memorized to memo, but in the different cluster
          // with the input node. Merge the two clusters.
          //
          // TODO: Traversal up the tree to do more merges.
          parent.memoTable.mergeClusters(cachedCluster, preparedCluster)
          // Since new node already memorized, we don't have to add it to either of the clusters
          // anymore.
          return
        }
        parent.addToCluster(preparedCluster, can)
      }

      override def memorize(node: T, constraintSet: PropertySet[T]): RasGroup[T] = {
        insert(node, constraintSet)
        parent.memoTable.groupOf(preparedCluster, constraintSet)
      }
    }
  }

  private class NodeToClusterMap[T <: AnyRef](cbo: Ras[T])
    extends CanonicalNodeMap[T, RasClusterKey](cbo)
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
  def cbo(): Ras[T]
  def clusterLookup(): Map[RasClusterKey, RasCluster[T]]
  def allClusters(): Iterable[RasCluster[T]]
  def allGroups(): Seq[RasGroup[T]]
}

object MemoState {
  def apply[T <: AnyRef](
                          cbo: Ras[T],
                          clusterLookup: Map[RasClusterKey, ImmutableRasCluster[T]],
                          allGroups: Seq[RasGroup[T]]): MemoState[T] = {
    MemoStateImpl(cbo, clusterLookup, allGroups)
  }

  private case class MemoStateImpl[T <: AnyRef](
                                                 override val cbo: Ras[T],
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
      GraphvizVisualizer(state.cbo(), state, best).format()
    }

    def formatGraphvizWithoutBest(rootGroupId: Int): String = {
      GraphvizVisualizer(state.cbo(), state, rootGroupId).format()
    }
  }
}
