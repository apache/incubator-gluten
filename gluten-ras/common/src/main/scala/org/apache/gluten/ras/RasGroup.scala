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

import org.apache.gluten.ras.memo.MemoStore
import org.apache.gluten.ras.property.PropertySet

trait RasGroup[T <: AnyRef] {
  def id(): Int
  def clusterKey(): RasClusterKey
  def constraintSet(): PropertySet[T]
  def self(): T
  def nodes(store: MemoStore[T]): Iterable[CanonicalNode[T]]
}

object RasGroup {
  def apply[T <: AnyRef](
      ras: Ras[T],
      clusterKey: RasClusterKey,
      id: Int,
      propSet: PropertySet[T]): RasGroup[T] = {
    new RasGroupImpl[T](ras, clusterKey, id, propSet)
  }

  private class RasGroupImpl[T <: AnyRef](
      ras: Ras[T],
      clusterKey: RasClusterKey,
      override val id: Int,
      override val constraintSet: PropertySet[T])
    extends RasGroup[T] {
    private val groupLeaf: T = ras.newGroupLeaf(id, clusterKey.metadata, constraintSet)

    override def clusterKey(): RasClusterKey = clusterKey
    override def self(): T = groupLeaf
    override def nodes(store: MemoStore[T]): Iterable[CanonicalNode[T]] = {
      store.getCluster(clusterKey).nodes().filter(n => n.propSet().satisfies(constraintSet))
    }
    override def toString(): String = {
      s"RasGroup(id=$id, clusterKey=$clusterKey, constraintSet=$constraintSet))"
    }
  }
}
