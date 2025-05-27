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

sealed trait MemoTable[T <: AnyRef] extends MemoStore[T] {
  import MemoTable._

  def ras: Ras[T]

  def allClusterKeys(): Seq[RasClusterKey]
  def allGroupIds(): Seq[Int]

  def getClusterPropSets(key: RasClusterKey): Set[PropertySet[T]]

  def probe(): Probe[T]

  def writeCount(): Int
}

object MemoTable {
  def create[T <: AnyRef](ras: Ras[T]): Writable[T] = ForwardMemoTable(ras)

  trait Writable[T <: AnyRef] extends MemoTable[T] {
    def newCluster(metadata: Metadata): RasClusterKey
    def groupOf(key: RasClusterKey, constraintSet: PropertySet[T]): RasGroup[T]

    def addToCluster(key: RasClusterKey, node: CanonicalNode[T]): Unit
    def mergeClusters(one: RasClusterKey, other: RasClusterKey): Unit
  }

  trait Probe[T <: AnyRef] {
    import Probe._
    def toDiff(): Diff[T]
  }

  object Probe {
    trait Diff[T <: AnyRef] {
      def changedClusters(): Set[RasClusterKey]
    }

    object Diff {
      def apply[T <: AnyRef](changedClusters: Set[RasClusterKey]): Diff[T] = DiffImpl(
        changedClusters)
      private case class DiffImpl[T <: AnyRef](override val changedClusters: Set[RasClusterKey])
        extends Diff[T]
    }
  }

  private case class MemoStateImpl[T <: AnyRef](
      override val ras: Ras[T],
      override val clusterLookup: Map[RasClusterKey, ImmutableRasCluster[T]],
      override val clusterHubGroupLookup: Map[RasClusterKey, RasGroup[T]],
      override val clusterUserGroupLookup: Map[RasClusterKey, RasGroup[T]],
      override val allGroups: Seq[RasGroup[T]],
      idToGroup: Map[Int, RasGroup[T]])
    extends MemoState[T] {
    private val allClustersCopy = clusterLookup.values

    override def getCluster(key: RasClusterKey): RasCluster[T] = clusterLookup(key)
    override def getHubGroup(key: RasClusterKey): RasGroup[T] = clusterHubGroupLookup(key)
    override def getUserGroup(key: RasClusterKey): RasGroup[T] = clusterUserGroupLookup(key)
    override def getGroup(id: Int): RasGroup[T] = idToGroup(id)
    override def allClusters(): Iterable[RasCluster[T]] = allClustersCopy
  }

  implicit class MemoTableImplicits[T <: AnyRef](table: MemoTable[T]) {
    def newState(): MemoState[T] = {
      val immutableClusters = table
        .allClusterKeys()
        .map(key => key -> ImmutableRasCluster(table.ras, table.getCluster(key)))
        .toMap

      val immutableHubGroups = table
        .allClusterKeys()
        .map(key => key -> table.getHubGroup(key))
        .toMap

      val immutableUserGroups = table
        .allClusterKeys()
        .map(key => key -> table.getUserGroup(key))
        .toMap

      var maxGroupId = Int.MinValue

      val groupMap = table
        .allGroupIds()
        .map {
          gid =>
            val group = table.getGroup(gid)
            assert(group.id() == gid)
            if (gid > maxGroupId) {
              maxGroupId = gid
            }
            gid -> group
        }
        .toMap

      val allGroups = (0 to maxGroupId).map(table.getGroup).toVector

      MemoStateImpl(
        table.ras,
        immutableClusters,
        immutableHubGroups,
        immutableUserGroups,
        allGroups,
        groupMap)
    }

    def doExhaustively(func: => Unit): Unit = {
      while (true) {
        val prevWriteCount = table.writeCount()
        func
        val writeCount = table.writeCount()
        assert(writeCount >= prevWriteCount)
        if (writeCount == prevWriteCount) {
          return
        }
      }
    }
  }
}
