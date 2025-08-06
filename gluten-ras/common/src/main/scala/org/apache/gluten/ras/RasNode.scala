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

import org.apache.gluten.ras.Ras.UnsafeHashKey
import org.apache.gluten.ras.property.PropertySet

trait RasNode[T <: AnyRef] {
  def ras(): Ras[T]
  def self(): T
  def propSet(): PropertySet[T]
}

object RasNode {
  implicit class RasNodeImplicits[T <: AnyRef](node: RasNode[T]) {
    def isCanonical: Boolean = {
      node.isInstanceOf[CanonicalNode[T]]
    }

    def asCanonical(): CanonicalNode[T] = {
      node.asInstanceOf[CanonicalNode[T]]
    }

    def isGroup: Boolean = {
      node.isInstanceOf[GroupNode[T]]
    }

    def asGroup(): GroupNode[T] = {
      node.asInstanceOf[GroupNode[T]]
    }

    def toHashKey: UnsafeHashKey[T] = node.ras().toHashKey(node.self())
  }
}

trait CanonicalNode[T <: AnyRef] extends RasNode[T] {
  def childrenCount: Int
}

object CanonicalNode {
  trait UniqueKey extends Any

  def apply[T <: AnyRef](ras: Ras[T], canonical: T): CanonicalNode[T] = {
    assert(ras.isCanonical(canonical))
    val propSet = ras.propSetOf(canonical)
    val children = ras.planModel.childrenOf(canonical)
    new CanonicalNodeImpl[T](ras, canonical, propSet, children.size)
  }

  // We put RasNode's API methods that accept mutable input in implicit definition.
  // Do not break this rule during further development.
  implicit class CanonicalNodeImplicits[T <: AnyRef](node: CanonicalNode[T]) {
    def isLeaf(): Boolean = {
      node.childrenCount == 0
    }

    def getChildrenGroups(allGroups: Int => RasGroup[T]): Seq[GroupNode[T]] = {
      val ras = node.ras()
      ras.getChildrenGroupIds(node.self()).map(allGroups(_)).map(g => GroupNode(ras, g))
    }

    def getChildrenGroupIds(): Seq[Int] = {
      val ras = node.ras()
      ras.getChildrenGroupIds(node.self())
    }
  }

  private class CanonicalNodeImpl[T <: AnyRef](
      override val ras: Ras[T],
      override val self: T,
      override val propSet: PropertySet[T],
      override val childrenCount: Int)
    extends CanonicalNode[T] {
    override def toString: String = ras.explain.describeNode(self)
    override def hashCode(): Int = throw new UnsupportedOperationException()
    override def equals(obj: Any): Boolean = throw new UnsupportedOperationException()
  }
}

trait GroupNode[T <: AnyRef] extends RasNode[T] {
  def groupId(): Int
}

object GroupNode {
  def apply[T <: AnyRef](ras: Ras[T], group: RasGroup[T]): GroupNode[T] = {
    val self = group.self()
    // Re-derive a property set of group leaf. User should define an appropriate conversion
    // from group constraints to its output properties in property model or plan model.
    val propSet = ras.propSetOf(self)
    new GroupNodeImpl[T](ras, self, propSet, group.id())
  }

  private class GroupNodeImpl[T <: AnyRef](
      override val ras: Ras[T],
      override val self: T,
      override val propSet: PropertySet[T],
      override val groupId: Int)
    extends GroupNode[T] {
    override def toString: String = ras.explain.describeNode(self)
    override def hashCode(): Int = throw new UnsupportedOperationException()
    override def equals(obj: Any): Boolean = throw new UnsupportedOperationException()
  }

  // We put RasNode's API methods that accept mutable input in implicit definition.
  // Do not break this rule during further development.
  implicit class GroupNodeImplicits[T <: AnyRef](gn: GroupNode[T]) {
    def group(allGroups: Int => RasGroup[T]): RasGroup[T] = {
      allGroups(gn.groupId())
    }
  }
}

trait InGroupNode[T <: AnyRef] {
  def groupId: Int
  def can: CanonicalNode[T]
}

object InGroupNode {
  def apply[T <: AnyRef](groupId: Int, node: CanonicalNode[T]): InGroupNode[T] = {
    InGroupNodeImpl(groupId, node)
  }

  private case class InGroupNodeImpl[T <: AnyRef](groupId: Int, can: CanonicalNode[T])
    extends InGroupNode[T]

  trait UniqueKey extends Any

  implicit class InGroupNodeImplicits[T <: AnyRef](n: InGroupNode[T]) {
    import InGroupNodeImplicits._
    def toUniqueKey: UniqueKey =
      InGroupNodeUniqueKeyImpl(n.groupId, System.identityHashCode(n.can))
  }

  private object InGroupNodeImplicits {
    private case class InGroupNodeUniqueKeyImpl(gid: Int, cid: Int) extends UniqueKey
  }
}

trait InClusterNode[T <: AnyRef] {
  def clusterKey: RasClusterKey
  def can: CanonicalNode[T]
}

object InClusterNode {
  def apply[T <: AnyRef](clusterId: RasClusterKey, node: CanonicalNode[T]): InClusterNode[T] = {
    InClusterNodeImpl(clusterId, node)
  }

  private case class InClusterNodeImpl[T <: AnyRef](
      clusterKey: RasClusterKey,
      can: CanonicalNode[T])
    extends InClusterNode[T]

  trait UniqueKey extends Any

  implicit class InClusterNodeImplicits[T <: AnyRef](n: InClusterNode[T]) {
    import InClusterNodeImplicits._
    def toUniqueKey: UniqueKey =
      InClusterNodeUniqueKeyImpl(n.clusterKey, System.identityHashCode(n.can))
  }

  private object InClusterNodeImplicits {
    private case class InClusterNodeUniqueKeyImpl(clusterKey: RasClusterKey, cid: Int)
      extends UniqueKey
  }
}
