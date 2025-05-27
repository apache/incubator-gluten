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

import org.apache.gluten.ras.memo.{MemoLike, MemoState}
import org.apache.gluten.ras.path.{PathFinder, RasPath}
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.ras.rule.EnforcerRuleFactory

import scala.collection.mutable.ArrayBuffer

object RasSuiteBase {
  trait TestNode {
    def selfCost(): Long
    def children(): Seq[TestNode]
    def withNewChildren(children: Seq[TestNode]): TestNode
  }

  trait UnaryLike extends TestNode {
    def child: TestNode
    def withNewChildren(child: TestNode): UnaryLike
    def children(): Seq[TestNode] = List(child)
    def withNewChildren(children: Seq[TestNode]): TestNode = withNewChildren(children.head)
  }

  trait BinaryLike extends TestNode {
    def left: TestNode
    def right: TestNode
    def children(): Seq[TestNode] = List(left, right)
    def withNewChildren(left: TestNode, right: TestNode): BinaryLike
    def withNewChildren(children: Seq[TestNode]): TestNode =
      withNewChildren(children.head, children(1))
  }

  trait LeafLike extends TestNode {
    def makeCopy(): LeafLike
    def children(): Seq[TestNode] = List.empty
    def withNewChildren(children: Seq[TestNode]): TestNode = this
  }

  case class Group private (id: Int, meta: Metadata, constraintSet: PropertySet[TestNode])
    extends LeafLike {
    override def selfCost(): Long = Long.MaxValue
    override def makeCopy(): LeafLike = copy()
  }

  object Group {
    def apply(id: Int): Group = {
      Group(id, MetadataModelImpl.DummyMetadata, PropertySet(List.empty))
    }

    def newBuilder(id: Int): Builder = {
      new Builder(id)
    }

    class Builder private[Group] (override val id: Int) extends GroupLeafBuilder[TestNode] {
      private var meta: Metadata = _
      private val constraints: ArrayBuffer[Property[TestNode]] = ArrayBuffer()

      def withMetadata(meta: Metadata): Builder = {
        this.meta = meta
        this
      }

      def withConstraint(constraint: Property[TestNode]): Builder = {
        this.constraints += constraint
        this
      }

      override def build(): TestNode = {
        Group(id, meta, PropertySet(constraints.toSeq))
      }
    }
  }

  case class LongCost(value: Long) extends Cost

  object CostModelImpl extends CostModel[TestNode] {

    override def costComparator(): Ordering[Cost] = {
      Ordering.Long.on { case LongCost(value) => value }
    }

    private def longCostOf(node: TestNode): Long = node match {
      case n: TestNode =>
        val selfCost = n.selfCost()

        // Sum with ceil to avoid overflow.
        def safeSum(a: Long, b: Long): Long = {
          val sum = a + b
          if (sum < a || sum < b) Long.MaxValue else sum
        }

        (n.children().map(longCostOf).toSeq :+ selfCost).reduce(safeSum)
      case _ => throw new IllegalStateException()
    }

    override def costOf(node: TestNode): Cost = node match {
      case g: Group => throw new IllegalStateException()
      case n => LongCost(longCostOf(n))
    }

    override def makeInfCost(): Cost = LongCost(Long.MaxValue)
  }

  object PlanModelImpl extends PlanModel[TestNode] {
    override def childrenOf(node: TestNode): Seq[TestNode] = node match {
      case n: TestNode => n.children()
      case _ => throw new IllegalStateException()
    }

    override def withNewChildren(node: TestNode, children: Seq[TestNode]): TestNode =
      node match {
        case n: TestNode => n.withNewChildren(children)
        case _ => throw new IllegalStateException()
      }

    override def hashCode(node: TestNode): Int = {
      java.util.Objects.hashCode(node)
    }

    override def equals(one: TestNode, other: TestNode): Boolean = {
      java.util.Objects.equals(one, other)
    }

    override def newGroupLeaf(groupId: Int): Group.Builder = Group.newBuilder(groupId)

    override def getGroupId(node: TestNode): Int = node match {
      case ngl: Group => ngl.id
      case _ => throw new IllegalStateException()
    }

    override def isGroupLeaf(node: TestNode): Boolean = node match {
      case _: Group => true
      case _ => false
    }
  }

  object ExplainImpl extends RasExplain[TestNode] {
    override def describeNode(node: TestNode): String = node match {
      case n => n.toString
    }
  }

  object MetadataModelImpl extends MetadataModel[TestNode] {
    case object DummyMetadata extends Metadata
    override def metadataOf(node: TestNode): Metadata = node match {
      case g: Group => throw new UnsupportedOperationException()
      case n: TestNode => DummyMetadata
      case other => throw new UnsupportedOperationException()
    }
    override def dummy(): Metadata = DummyMetadata
    override def verify(one: Metadata, other: Metadata): Unit = {
      assert(one == DummyMetadata)
      assert(other == DummyMetadata)
    }

    override def assignToGroup(group: GroupLeafBuilder[TestNode], meta: Metadata): Unit = {
      (group, meta) match {
        case (builder: Group.Builder, m: Metadata) =>
          builder.withMetadata(m)
      }
    }
  }

  object PropertyModelImpl extends PropertyModel[TestNode] {
    override def propertyDefs: Seq[PropertyDef[TestNode, _ <: Property[TestNode]]] = Seq.empty
    override def newEnforcerRuleFactory(): EnforcerRuleFactory[TestNode] =
      (_: PropertySet[TestNode]) => Seq.empty
  }

  implicit class GraphvizPrinter[T <: AnyRef](val planner: RasPlanner[T]) {
    def printGraphviz(): Unit = {
      // scalastyle:off println
      println(planner.newState().formatGraphviz())
      // scalastyle:on println
    }
  }

  implicit class MemoLikeImplicits[T <: AnyRef](val memo: MemoLike[T]) {
    def memorize(ras: Ras[T], node: T): RasGroup[T] = {
      memo.memorize(node, ras.userConstraintSet())
    }
  }

  implicit class MemoStateImplicits[T <: AnyRef](val state: MemoState[T]) {
    def getGroupCount(): Int = {
      state.allGroups().size
    }

    def collectAllPaths(depth: Int): Iterable[RasPath[T]] = {
      val allClusters = state.allClusters()
      val allGroups = state.allGroups()
      val hubGroupLookup = state.clusterHubGroupLookup()

      val highestFinder = PathFinder
        .builder(state.ras(), state)
        .depth(depth)
        .build()

      hubGroupLookup.flatMap {
        case (cKey, hubGroup) =>
          val hubGroupNode = GroupNode(state.ras(), hubGroup)
          val highest = highestFinder.find(hubGroupNode).maxBy(c => c.height())
          val finder = (1 to highest.height())
            .foldLeft(PathFinder
              .builder(state.ras(), state)) {
              case (builder, d) =>
                builder.depth(d)
            }
            .build()
          finder.find(hubGroupNode)
      }
    }
  }

  implicit class TestNodeImplicits(val node: TestNode) {
    def asCanonical(ras: Ras[TestNode]): CanonicalNode[TestNode] = {
      CanonicalNode(ras, node)
    }
  }

  implicit class TestNodeGroupImplicits(val group: RasGroup[TestNode]) {
    def asGroup(ras: Ras[TestNode]): GroupNode[TestNode] = {
      GroupNode(ras, group)
    }
  }
}
