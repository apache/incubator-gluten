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

import org.apache.gluten.ras.RasConfig.PlannerType
import org.apache.gluten.ras.RasSuiteBase._
import org.apache.gluten.ras.rule.{RasRule, Shape, Shapes}

import org.scalatest.funsuite.AnyFunSuite

class ExhaustiveMetadataSuite extends MetadataSuite {
  override protected def conf: RasConfig = RasConfig(plannerType = PlannerType.Exhaustive)
}

class DpMetadataSuite extends MetadataSuite {
  override protected def conf: RasConfig = RasConfig(plannerType = PlannerType.Dp)
}

abstract class MetadataSuite extends AnyFunSuite {
  import MetadataSuite._
  protected def conf: RasConfig

  test("Dry run") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        RowCountMetadataModel,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.none())
        .withNewConfig(_ => conf)

    val planner = ras.newPlanner(KnownRowCountUnary(0.5, KnownRowCountLeaf(2000)))
    val out = planner.plan()
    assert(out == KnownRowCountUnary(0.5, KnownRowCountLeaf(2000)))
  }

  test("Trivial planning") {
    object CombineUnaryNodes extends RasRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case KnownRowCountUnary(0.25d, KnownRowCountUnary(2.0d, child)) =>
          assert(child.isInstanceOf[Group])
          assert(child.asInstanceOf[Group].meta.isInstanceOf[IntRowCount])
          assert(child.asInstanceOf[Group].meta.asInstanceOf[IntRowCount].value == 2000)
          List(KnownRowCountUnary(0.5d, child))
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(2)
    }

    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        RowCountMetadataModel,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List(CombineUnaryNodes)))
        .withNewConfig(_ => conf)

    val planner =
      ras.newPlanner(KnownRowCountUnary(0.25d, KnownRowCountUnary(2.0d, KnownRowCountLeaf(2000))))
    val out = planner.plan()
    assert(out == KnownRowCountUnary(0.5d, KnownRowCountLeaf(2000)))
  }

  test("Cluster merge") {
    object CombineUnaryNodes extends RasRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case KnownRowCountUnary(0.25d, KnownRowCountUnary(2.0d, child)) =>
          assert(child.isInstanceOf[Group])
          assert(child.asInstanceOf[Group].meta.isInstanceOf[IntRowCount])
          assert(child.asInstanceOf[Group].meta.asInstanceOf[IntRowCount].value == 2000)
          List(KnownRowCountUnary(0.5d, child))
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(2)
    }

    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        RowCountMetadataModel,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List(CombineUnaryNodes)))
        .withNewConfig(_ => conf)

    val in = KnownRowCountBinary(
      KnownRowCountUnary(0.5d, KnownRowCountLeaf(2000)),
      KnownRowCountUnary(0.25d, KnownRowCountUnary(2.0d, KnownRowCountLeaf(2000))))
    val planner = ras.newPlanner(in)
    val out = planner.plan()
    assert(
      out == KnownRowCountBinary(
        KnownRowCountUnary(0.5, KnownRowCountLeaf(2000)),
        KnownRowCountUnary(0.5, KnownRowCountLeaf(2000))))
  }
}

object MetadataSuite {
  private object RowCountMetadataModel extends MetadataModel[TestNode] {
    override def metadataOf(node: TestNode): Metadata = node match {
      case n: KnownRowCountNode =>
        IntRowCount(n.rowCount())
      case other =>
        throw new UnsupportedOperationException()
    }

    override def dummy(): Metadata = IntRowCount(0)
    override def verify(one: Metadata, other: Metadata): Unit = (one, other) match {
      case (IntRowCount(a), IntRowCount(b)) =>
        assert(a == b)
      case other =>
        throw new UnsupportedOperationException()
    }

    override def assignToGroup(group: GroupLeafBuilder[TestNode], meta: Metadata): Unit = {
      (group, meta) match {
        case (builder: Group.Builder, m: Metadata) =>
          builder.withMetadata(m)
      }
    }
  }

  trait RowCount extends Metadata

  case class IntRowCount(value: Int) extends RowCount

  trait KnownRowCountNode extends TestNode {
    def rowCount(): Int
  }

  case class KnownRowCountUnary(selectivity: Double, override val child: TestNode)
    extends UnaryLike
    with KnownRowCountNode {
    private val childRowCount = child match {
      case n: KnownRowCountNode => n.rowCount()
      case g: Group => g.meta.asInstanceOf[IntRowCount].value
      case other => throw new UnsupportedOperationException()
    }

    override def withNewChildren(child: TestNode): UnaryLike = copy(selectivity, child)
    override def rowCount(): Int = (childRowCount * selectivity).toInt
    override def selfCost(): Long = childRowCount
  }

  case class KnownRowCountLeaf(rowCount: Int) extends LeafLike with KnownRowCountNode {
    override def makeCopy(): LeafLike = this
    override def selfCost(): Long = rowCount
  }

  case class KnownRowCountBinary(override val left: TestNode, override val right: TestNode)
    extends BinaryLike
    with KnownRowCountNode {
    private val leftRowCount = left match {
      case n: KnownRowCountNode => n.rowCount()
      case g: Group => g.meta.asInstanceOf[IntRowCount].value
      case other => throw new UnsupportedOperationException()
    }

    private val rightRowCount = right match {
      case n: KnownRowCountNode => n.rowCount()
      case g: Group => g.meta.asInstanceOf[IntRowCount].value
      case other => throw new UnsupportedOperationException()
    }

    override def withNewChildren(left: TestNode, right: TestNode): BinaryLike = copy(left, right)
    override def rowCount(): Int = leftRowCount * rightRowCount
    override def selfCost(): Long = leftRowCount + rightRowCount
  }
}
