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
import org.apache.gluten.ras.RasCluster.MutableRasCluster
import org.apache.gluten.ras.memo.MemoTable.Probe
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.ras.util.IndexDisjointSet

import scala.collection.mutable

class ForwardMemoTable[T <: AnyRef] private (override val ras: Ras[T])
  extends MemoTable.Writable[T] {
  import ForwardMemoTable._

  private val groupBuffer: mutable.ArrayBuffer[RasGroup[T]] = mutable.ArrayBuffer()

  private val clusterKeyBuffer: mutable.ArrayBuffer[IntClusterKey] = mutable.ArrayBuffer()
  private val clusterBuffer: mutable.ArrayBuffer[MutableRasCluster[T]] = mutable.ArrayBuffer()
  private val clusterDisjointSet: IndexDisjointSet = IndexDisjointSet()

  private val groupLookup: mutable.ArrayBuffer[mutable.Map[PropertySet[T], RasGroup[T]]] =
    mutable.ArrayBuffer()

  private val clusterMergeLog: mutable.ArrayBuffer[(Int, Int)] = mutable.ArrayBuffer()
  private var memoWriteCount: Int = 0

  override def getCluster(key: RasClusterKey): MutableRasCluster[T] = {
    val ancestor = ancestorClusterIdOf(key)
    clusterBuffer(ancestor)
  }

  override def newCluster(metadata: Metadata): RasClusterKey = {
    checkBufferSizes()
    val clusterId = clusterBuffer.size
    val key = IntClusterKey(clusterId, metadata)
    clusterKeyBuffer += key
    clusterBuffer += MutableRasCluster(ras, metadata)
    clusterDisjointSet.grow()
    groupLookup += mutable.Map()
    groupOf(key, ras.hubConstraintSet())
    groupOf(key, ras.userConstraintSet())
    memoWriteCount += 1
    key
  }

  override def groupOf(key: RasClusterKey, constraintSet: PropertySet[T]): RasGroup[T] = {
    val ancestor = ancestorClusterIdOf(key)
    val lookup = groupLookup(ancestor)
    if (lookup.contains(constraintSet)) {
      return lookup(constraintSet)
    }
    val gid = groupBuffer.size
    val newGroup =
      RasGroup(ras, IntClusterKey(ancestor, key.metadata), gid, constraintSet)
    lookup += constraintSet -> newGroup
    groupBuffer += newGroup
    memoWriteCount += 1
    newGroup
  }

  override def getClusterPropSets(key: RasClusterKey): Set[PropertySet[T]] = {
    val ancestor = ancestorClusterIdOf(key)
    groupLookup(ancestor).keySet.toSet
  }

  override def addToCluster(key: RasClusterKey, node: CanonicalNode[T]): Unit = {
    val cluster = getCluster(key)
    if (cluster.contains(node)) {
      return
    }
    cluster.add(node)
    memoWriteCount += 1
  }

  override def mergeClusters(one: RasClusterKey, other: RasClusterKey): Unit = {
    val oneAncestor = ancestorClusterIdOf(one)
    val otherAncestor = ancestorClusterIdOf(other)

    if (oneAncestor == otherAncestor) {
      // Already merged.
      return
    }

    case class Merge(from: RasClusterKey, to: RasClusterKey) {
      ras.metadataModel.verify(from.metadata, to.metadata)
    }

    val merge = if (oneAncestor > otherAncestor) {
      Merge(clusterKeyBuffer(oneAncestor), clusterKeyBuffer(otherAncestor))
    } else {
      Merge(clusterKeyBuffer(otherAncestor), clusterKeyBuffer(oneAncestor))
    }

    val fromKey = merge.from
    val toKey = merge.to

    val fromCluster = clusterBuffer(fromKey.id())
    val toCluster = clusterBuffer(toKey.id())

    // Add absent nodes.
    fromCluster.nodes().foreach {
      fromNode =>
        if (!toCluster.contains(fromNode)) {
          toCluster.add(fromNode)
        }
    }

    // Add absent groups.
    val fromGroups = groupLookup(fromKey.id())
    val toGroups = groupLookup(toKey.id())
    fromGroups.foreach {
      case (fromConstraintSet, _) =>
        if (!toGroups.contains(fromConstraintSet)) {
          groupOf(toKey, fromConstraintSet)
        }
    }

    // Forward the element in disjoint set.
    clusterDisjointSet.forward(fromKey.id(), toKey.id())
    clusterMergeLog += (fromKey.id() -> toKey.id())
    memoWriteCount += 1
  }

  override def getGroup(id: Int): RasGroup[T] = {
    assert(id >= 0)
    groupBuffer(id)
  }

  override def allClusterKeys(): Seq[RasClusterKey] = clusterKeyBuffer.toSeq

  override def allGroupIds(): Seq[Int] = {
    val from = 0
    val to = groupBuffer.size
    (from until to).toVector
  }

  private def ancestorClusterIdOf(key: RasClusterKey): Int = {
    clusterDisjointSet.find(key.id())
  }

  private def checkBufferSizes(): Unit = {
    assert(clusterKeyBuffer.size == clusterBuffer.size)
    assert(clusterKeyBuffer.size == clusterDisjointSet.size)
    assert(clusterKeyBuffer.size == groupLookup.size)
  }

  override def probe(): MemoTable.Probe[T] = new ForwardMemoTable.Probe[T](this)

  override def writeCount(): Int = memoWriteCount

  override def getHubGroup(key: RasClusterKey): RasGroup[T] = {
    val ancestor = ancestorClusterIdOf(key)
    val lookup = groupLookup(ancestor)
    lookup(ras.hubConstraintSet())
  }

  override def getUserGroup(key: RasClusterKey): RasGroup[T] = {
    val ancestor = ancestorClusterIdOf(key)
    val lookup = groupLookup(ancestor)
    lookup(ras.userConstraintSet())
  }
}

object ForwardMemoTable {
  def apply[T <: AnyRef](ras: Ras[T]): MemoTable.Writable[T] = new ForwardMemoTable[T](ras)

  private case class IntClusterKey(id: Int, metadata: Metadata) extends RasClusterKey

  private class Probe[T <: AnyRef](table: ForwardMemoTable[T]) extends MemoTable.Probe[T] {
    private val probedClusterCount: Int = table.clusterKeyBuffer.size
    private val probedGroupCount: Int = table.groupBuffer.size
    private val probedMergeLogSize: Int = table.clusterMergeLog.size

    override def toDiff(): Probe.Diff[T] = {
      val newClusterCount = table.clusterKeyBuffer.size
      val newGroupCount = table.groupBuffer.size
      val newMergeLogSize = table.clusterMergeLog.size

      assert(newClusterCount >= probedClusterCount)
      assert(newGroupCount >= probedGroupCount)
      assert(newMergeLogSize >= probedMergeLogSize)

      // Find new clusters.
      val newClusters = table.clusterKeyBuffer.slice(probedClusterCount, newClusterCount)

      // Find resident clusters of the new groups.
      val newGroups = table.groupBuffer.slice(probedGroupCount, newGroupCount)
      val clustersOfNewGroups = newGroups.map(g => g.clusterKey())

      // Find all the affected clusters, if cluster-merge happened.
      val newMergeLogs = table.clusterMergeLog.slice(probedMergeLogSize, newMergeLogSize)
      val affectedClustersDuringMerging = newMergeLogs
        .flatMap {
          case (from, to) =>
            table.clusterDisjointSet.setOf(to)
        }
        .map(index => table.clusterKeyBuffer(index))

      val changedClusters =
        (clustersOfNewGroups.toSet ++ affectedClustersDuringMerging) -- newClusters
      // We consider an existing cluster with new groups changed.
      Probe.Diff(changedClusters)
    }
  }

  implicit class RasClusterKeyImplicits(key: RasClusterKey) {
    def id(): Int = {
      asIntKey().id
    }

    private def asIntKey(): IntClusterKey = {
      key.asInstanceOf[IntClusterKey]
    }
  }
}
