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
package org.apache.gluten.ras.mock

import org.apache.gluten.ras._
import org.apache.gluten.ras.RasSuiteBase._
import org.apache.gluten.ras.memo.{MemoState, MemoStore}
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.ras.vis.GraphvizVisualizer

import scala.collection.mutable

case class MockMemoState[T <: AnyRef] private (
    override val ras: Ras[T],
    override val clusterLookup: Map[RasClusterKey, RasCluster[T]],
    override val allGroups: Seq[RasGroup[T]])
  extends MemoState[T] {
  def printGraphviz(group: RasGroup[T]): Unit = {
    val graph = GraphvizVisualizer(ras, this, group.id())
    // scalastyle:off println
    println(graph.format())
    // scalastyle:on println
  }

  def printGraphviz(best: Best[T]): Unit = {
    val graph = vis.GraphvizVisualizer(ras, this, best)
    // scalastyle:off println
    println(graph.format())
    // scalastyle:on println
  }

  override def allClusters(): Iterable[RasCluster[T]] = clusterLookup.values

  override def getCluster(key: RasClusterKey): RasCluster[T] = clusterLookup(key)

  override def getGroup(id: Int): RasGroup[T] = allGroups(id)

  override def clusterDummyGroupLookup(): Map[RasClusterKey, RasGroup[T]] = Map.empty

  override def getDummyGroup(key: RasClusterKey): RasGroup[T] =
    throw new UnsupportedOperationException()
}

object MockMemoState {
  class Builder[T <: AnyRef] private (ras: Ras[T]) {
    private var propSet: PropertySet[T] = PropertySet[T](List.empty)
    private val clusterBuffer = mutable.Map[RasClusterKey, MockMutableCluster[T]]()
    private val groupFactory: MockMutableGroup.Factory[T] =
      MockMutableGroup.Factory.create[T](ras, propSet)

    def withPropertySet(propSet: PropertySet[T]): Builder[T] = {
      this.propSet = propSet
      this
    }

    def newCluster(): MockMutableCluster[T] = {
      val id = clusterBuffer.size
      val key = MockMutableCluster.DummyIntClusterKey(id)
      val cluster = MockMutableCluster[T](ras, key, groupFactory)
      clusterBuffer += (key -> cluster)
      cluster
    }

    def build(): MockMemoState[T] = {
      MockMemoState[T](ras, clusterBuffer.toMap, groupFactory.allGroups())
    }
  }

  object Builder {
    def apply[T <: AnyRef](ras: Ras[T]): Builder[T] = {
      new Builder[T](ras)
    }
  }

  // TODO add groups with different property sets
  class MockMutableCluster[T <: AnyRef] private (
      ras: Ras[T],
      key: RasClusterKey,
      groupFactory: MockMutableGroup.Factory[T])
    extends RasCluster[T] {
    private val nodeBuffer = mutable.ArrayBuffer[CanonicalNode[T]]()

    def newGroup(): MockMutableGroup[T] = {
      groupFactory.newGroup(key)
    }

    def addNodes(nodes: Seq[CanonicalNode[T]]): Unit = {
      nodeBuffer ++= nodes
    }

    override def nodes(): Seq[CanonicalNode[T]] = nodeBuffer
  }

  object MockMutableCluster {
    def apply[T <: AnyRef](
        ras: Ras[T],
        key: RasClusterKey,
        groupFactory: MockMutableGroup.Factory[T]): MockMutableCluster[T] = {
      new MockMutableCluster[T](ras, key, groupFactory)
    }

    case class DummyIntClusterKey(id: Int) extends RasClusterKey {
      override def metadata: Metadata = MetadataModelImpl.DummyMetadata
    }
  }

  class MockMutableGroup[T <: AnyRef] private (
      override val id: Int,
      override val clusterKey: RasClusterKey,
      override val propSet: PropertySet[T],
      override val self: T)
    extends RasGroup[T] {
    private val nodes: mutable.ArrayBuffer[CanonicalNode[T]] = mutable.ArrayBuffer()

    def add(node: CanonicalNode[T]): Unit = {
      nodes += node
    }

    def add(newNodes: Seq[CanonicalNode[T]]): Unit = {
      nodes ++= newNodes
    }

    override def nodes(store: MemoStore[T]): Iterable[CanonicalNode[T]] = nodes
  }

  object MockMutableGroup {
    class Factory[T <: AnyRef] private (ras: Ras[T], propSet: PropertySet[T]) {
      private val groupBuffer = mutable.ArrayBuffer[MockMutableGroup[T]]()

      def newGroup(clusterKey: RasClusterKey): MockMutableGroup[T] = {
        val id = groupBuffer.size
        val group =
          new MockMutableGroup[T](
            id,
            clusterKey,
            propSet,
            ras.planModel.newGroupLeaf(id, clusterKey.metadata, propSet))
        groupBuffer += group
        group
      }

      def allGroups(): Seq[MockMutableGroup[T]] = groupBuffer
    }

    object Factory {
      def create[T <: AnyRef](ras: Ras[T], propSet: PropertySet[T]): Factory[T] = {
        new Factory[T](ras, propSet)
      }
    }
  }

}
