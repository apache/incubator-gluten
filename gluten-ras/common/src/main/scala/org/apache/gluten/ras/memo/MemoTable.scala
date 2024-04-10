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

  override def getCluster(key: RasClusterKey): RasCluster[T]
  override def getGroup(id: Int): RasGroup[T]

  def allClusters(): Seq[RasClusterKey]
  def allGroups(): Seq[RasGroup[T]]
  def allDummyGroups(): Seq[RasGroup[T]]

  def getClusterPropSets(key: RasClusterKey): Set[PropertySet[T]]

  def probe(): Probe[T]

  def writeCount(): Int
}

object MemoTable {
  def create[T <: AnyRef](ras: Ras[T]): Writable[T] = ForwardMemoTable(ras)

  trait Writable[T <: AnyRef] extends MemoTable[T] {
    def newCluster(metadata: Metadata): RasClusterKey
    def groupOf(key: RasClusterKey, propertySet: PropertySet[T]): RasGroup[T]

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

  implicit class MemoTableImplicits[T <: AnyRef](table: MemoTable[T]) {
    def newState(): MemoState[T] = {
      val immutableClusters = table
        .allClusters()
        .map(key => key -> ImmutableRasCluster(table.ras, table.getCluster(key)))
        .toMap
      val immutableDummyGroups = table
        .allClusters()
        .map(key => key -> table.getDummyGroup(key))
        .toMap
      table.allDummyGroups().zipWithIndex.foreach {
        case (group, idx) =>
          assert(group.id() == -(idx + 1))
      }
      MemoState(
        table.ras,
        immutableClusters,
        immutableDummyGroups,
        table.allGroups(),
        table.allDummyGroups())
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
