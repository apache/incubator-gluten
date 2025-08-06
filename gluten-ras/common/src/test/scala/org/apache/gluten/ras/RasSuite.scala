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
import org.apache.gluten.ras.memo.Memo
import org.apache.gluten.ras.path.Pattern
import org.apache.gluten.ras.path.Pattern.Matchers
import org.apache.gluten.ras.rule.{RasRule, Shape, Shapes}

import org.scalatest.funsuite.AnyFunSuite

class ExhaustivePlannerRasSuite extends RasSuite {
  override protected def conf: RasConfig = RasConfig(plannerType = PlannerType.Exhaustive)
}

class DpPlannerRasSuite extends RasSuite {
  override protected def conf: RasConfig = RasConfig(plannerType = PlannerType.Dp)
}

abstract class RasSuite extends AnyFunSuite {
  import RasSuite._

  protected def conf: RasConfig

  test("Group memo - re-memorize") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.none())
        .withNewConfig(_ => conf)
    val memo = Memo(ras)
    val group1 = memo.memorize(ras, Unary(50, Unary(50, Leaf(30))))
    val group2 = memo.memorize(ras, Unary(50, Unary(50, Leaf(30))))
    assert(group2 eq group1)
  }

  test("Group memo - define equivalence") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.none())
        .withNewConfig(_ => conf)
    val memo = Memo(ras)
    val group = memo.memorize(ras, Unary(50, Unary(50, Leaf(30))))
    val state = memo.newState()
    assert(group.nodes(state).size == 1)
    memo.openFor(group.clusterKey()).memorize(ras, Unary(30, Leaf(90)))
    assert(memo.newState().allGroups().size == 8)
  }

  test("Group memo - define equivalence: binary with similar children, 1") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.none())
        .withNewConfig(_ => conf)
    val memo = Memo(ras)
    val group = memo.memorize(ras, Binary(50, Leaf(30), Leaf(40)))
    val state = memo.newState()
    assert(group.nodes(state).size == 1)
    val leaf40Group = memo.memorize(ras, Leaf(40))
    assert(leaf40Group.nodes(state).size == 1)
    memo.openFor(leaf40Group.clusterKey()).memorize(ras, Leaf(30))
    assert(memo.newState().allGroups().size == 6)
  }

  test("Group memo - define equivalence: binary with similar children, 2") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.none())
        .withNewConfig(_ => conf)
    val memo = Memo(ras)
    val group = memo.memorize(ras, Binary(50, Unary(20, Leaf(30)), Unary(20, Leaf(40))))
    val state = memo.newState()
    assert(group.nodes(state).size == 1)
    val leaf40Group = memo.memorize(ras, Leaf(40))
    assert(leaf40Group.nodes(state).size == 1)
    memo.openFor(leaf40Group.clusterKey()).memorize(ras, Leaf(30))
    assert(memo.newState().allGroups().size == 10)
  }

  test("Group memo - partial canonical") {
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.none())
        .withNewConfig(_ => conf)
    val memo = Memo(ras)
    val group1 = memo.memorize(ras, Unary(50, Unary(50, Leaf(30))))
    val group2 = memo.memorize(ras, Unary(50, Leaf(30)))
    val group3 = memo.memorize(ras, Unary(50, Group(group2.id())))
    assert(group3 eq group1)
  }

  test(s"Unary node") {
    object DivideUnaryCost extends RasRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case Unary(cost, child) =>
          if (cost >= 35) {
            val halfCost = cost / 3
            List(Unary(halfCost, Unary(halfCost, child)))
          } else {
            List.empty
          }
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    object DecreaseUnaryCost extends RasRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case Unary(cost, child) =>
          if (cost >= 80) {
            List(Unary(cost - 20, child))
          } else {
            List.empty
          }
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List(DivideUnaryCost, DecreaseUnaryCost)))
        .withNewConfig(_ => conf)
    val plan = Unary(90, Leaf(70))
    val planner = ras.newPlanner(plan)
    val optimized = planner.plan()

    assert(optimized == Unary(23, Unary(23, Leaf(70))))
  }

  test(s"Group reduction") {
    object RemoveUnary extends RasRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case Unary(cost, child) => List(child)
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List(RemoveUnary)))
        .withNewConfig(_ => conf)
    val plan = Unary(60, Unary(90, Leaf(70)))
    val planner = ras.newPlanner(plan)
    val optimized = planner.plan()

    assert(optimized == Leaf(70))
  }

  test(s"Group expansion - fixed height") {
    object AddUnary extends RasRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = {
        assert(node.isInstanceOf[Group])
        List(Unary(50, node))
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(0)
    }

    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List(AddUnary)))
        .withNewConfig(_ => conf)
    val plan = Unary(60, Unary(90, Leaf(70)))
    val planner = ras.newPlanner(plan)
    val optimized = planner.plan()

    assert(optimized == Unary(60, Unary(90, Leaf(70))))

    val state = planner.newState().memoState()
    val allPaths = state.collectAllPaths(Int.MaxValue)

    assert(state.allClusters().size == 3)
    assert(state.allGroups().size == 6)
    assert(allPaths.size == 33)
  }

  test(s"Group expansion - pattern") {
    object AddUnary extends RasRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = {
        assert(node.isInstanceOf[Group])
        List(Unary(50, node))
      }

      override def shape(): Shape[TestNode] = Shapes.pattern(Pattern.ignore.build())
    }

    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List(AddUnary)))
        .withNewConfig(_ => conf)
    val plan = Unary(60, Unary(90, Leaf(70)))
    val planner = ras.newPlanner(plan)
    val optimized = planner.plan()

    assert(optimized == Unary(60, Unary(90, Leaf(70))))

    val state = planner.newState().memoState()
    val allPaths = state.collectAllPaths(Int.MaxValue)

    assert(state.allClusters().size == 3)
    assert(state.allGroups().size == 6)
    assert(allPaths.size == 33)
  }

  test(s"Rule dependency") {
    // Op3 relies on Op2 relies on Op1

    object Op1 extends RasRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case Leaf(70) =>
          List(Leaf(69))
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    object Op2 extends RasRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case Leaf(69) =>
          List(Leaf(68))
        case other => List.empty
      }

      override def shape(): Shape[TestNode] =
        Shapes.pattern(Pattern.leaf[TestNode](Matchers.clazz(classOf[Leaf])).build())
    }

    object Op3 extends RasRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case Leaf(68) =>
          List(Leaf(67))
        case other => List.empty
      }

      override def shape(): Shape[TestNode] =
        Shapes.pattern(Pattern.any[TestNode].build())
    }

    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List(Op3, Op1, Op2)))
        .withNewConfig(_ => conf)

    val plan = Unary(90, Unary(90, Leaf(70)))
    val planner = ras.newPlanner(plan)
    val optimized = planner.plan()

    assert(optimized == Unary(90, Unary(90, Leaf(67))))
  }

  test(s"Unary node insertion") {
    object InsertUnary2 extends RasRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case Unary(cost1, Unary(cost2, child)) =>
          List(Unary(cost1 - 11, Unary2(10, Unary(cost2, child))))
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(2)
    }

    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List(InsertUnary2)))
        .withNewConfig(_ => conf)

    val plan = Unary(90, Unary(90, Leaf(70)))
    val planner = ras.newPlanner(plan)
    val optimized = planner.plan()

    assert(optimized == Unary(79, Unary2(10, Unary(90, Leaf(70)))))
  }

  test(s"Binary node") {
    object DivideBinaryCost extends RasRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case Binary(cost, left, right) =>
          if (cost >= 35) {
            val halfCost = cost / 3
            List(Binary(halfCost, Binary(halfCost, left, right), Binary(halfCost, left, right)))
          } else {
            List.empty
          }
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List(DivideBinaryCost)))
        .withNewConfig(_ => conf)

    val plan = Binary(90, Leaf(70), Leaf(70))
    val planner = ras.newPlanner(plan)
    val optimized = planner.plan()

    assert(optimized == Binary(90, Leaf(70), Leaf(70)))
  }

  test(s"Symmetric rule") {
    object SymmetricRule extends RasRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case Unary(cost, child) => List(Unary2(cost, child))
        case Unary2(cost, child) => List(Unary(cost, child))
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List(SymmetricRule)))
        .withNewConfig(_ => conf)

    val plan = Unary(90, Leaf(70))
    val planner = ras.newPlanner(plan)
    val optimized = planner.plan()
    val state = planner.newState()

    // The 2 plans have the same cost.
    assert(optimized == Unary(90, Leaf(70)) || optimized == Unary2(90, Leaf(70)))
    assert(state.memoState().getGroupCount() == 4)
  }

  test(s"Binary swap") {
    object BinarySwap extends RasRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case Binary(cost, left, right) if cost >= 1 =>
          List(Binary((cost - 1).max(0), right, left))
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List(BinarySwap)))
        .withNewConfig(_ => conf)

    val plan = Binary(90, Leaf(50), Leaf(70))
    val planner = ras.newPlanner(plan)
    val optimized = planner.plan()

    assert(optimized == Binary(0, Leaf(50), Leaf(70)))
  }

  test(s"Binary swap equivalent leaves") {
    object BinarySwap extends RasRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case Binary(cost, left, right) if cost >= 1 =>
          List(Binary((cost - 1).max(0), right, left))
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List(BinarySwap)))
        .withNewConfig(_ => conf)

    val plan = Binary(70, Binary(90, Leaf(50), Leaf(50)), Leaf(50))
    val planner = ras.newPlanner(plan)
    val optimized = planner.plan()

    assert(optimized == Binary(0, Binary(0, Leaf(50), Leaf(50)), Leaf(50)))
  }

  test(s"Avoid unused groups") {
    object Unary2Unary3 extends RasRule[TestNode] {
      override def shift(node: TestNode): Iterable[TestNode] = node match {
        case Unary2(cost, child) if cost >= 1 => List(Unary3(cost - 1, child))
        case Unary3(cost, child) if cost >= 1 => List(Unary2(cost + 1, child))
        case other => List.empty
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
    }

    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List(Unary2Unary3)))
        .withNewConfig(_ => conf)

    val plan = Unary(50, Unary2(50, Leaf(30)))
    val planner = ras.newPlanner(plan)
    val optimized = planner.plan()
    val state = planner.newState()

    assert(state.memoState().getGroupCount() == 6)
    assert(optimized == Unary(50, Unary3(49, Leaf(30))))
  }

  test(s"Rule application depth - depth 1") {
    val l2l2 = new LeafToLeaf2()
    val u2u2 = new UnaryToUnary2()
    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List(l2l2, u2u2)))
        .withNewConfig(_ => conf)

    val plan = Unary(50, Unary2(50, Unary2(50, Unary2(50, Leaf(30)))))
    val planner = ras.newPlanner(plan)
    val optimized = planner.plan()

    assert(optimized == Unary2(49, Unary2(50, Unary2(50, Unary2(50, Leaf2(29))))))
  }

  test(s"Rule application depth - depth 2") {
    val l2l2 = new LeafToLeaf2()
    val u2u2 = new UnaryToUnary2()

    object Unary2Unary2ToUnary3 extends RasRule[TestNode] {
      var invocationCount: Int = 0
      var effectiveInvocationCount: Int = 0
      override def shift(node: TestNode): Iterable[TestNode] = {
        invocationCount += 1
        node match {
          case Unary2(cost1, Unary2(cost2, child)) =>
            effectiveInvocationCount += 1
            List(Unary3(cost1 + cost2 - 1, child))
          case other => List.empty
        }
      }

      override def shape(): Shape[TestNode] = Shapes.fixedHeight(2)
    }

    val ras =
      Ras[TestNode](
        PlanModelImpl,
        CostModelImpl,
        MetadataModelImpl,
        PropertyModelImpl,
        ExplainImpl,
        RasRule.Factory.reuse(List(l2l2, u2u2, Unary2Unary2ToUnary3)))
        .withNewConfig(_ => conf)

    val plan = Unary(50, Unary2(50, Unary2(50, Unary2(50, Leaf(30)))))
    val planner = ras.newPlanner(plan)
    val optimized = planner.plan()

    assert(Unary2Unary2ToUnary3.invocationCount == 14)
    assert(Unary2Unary2ToUnary3.effectiveInvocationCount == 3)
    assert(optimized == Unary3(98, Unary3(99, Leaf2(29))))
  }
}

object RasSuite {

  case class Binary(
      override val selfCost: Long,
      override val left: TestNode,
      override val right: TestNode)
    extends BinaryLike {
    override def withNewChildren(left: TestNode, right: TestNode): BinaryLike =
      copy(left = left, right = right)
  }

  case class Unary(override val selfCost: Long, override val child: TestNode) extends UnaryLike {
    override def withNewChildren(child: TestNode): UnaryLike = copy(child = child)
  }

  case class Unary2(override val selfCost: Long, override val child: TestNode) extends UnaryLike {
    override def withNewChildren(child: TestNode): UnaryLike = copy(child = child)
  }

  case class Unary3(override val selfCost: Long, override val child: TestNode) extends UnaryLike {
    override def withNewChildren(child: TestNode): UnaryLike = copy(child = child)
  }

  case class Leaf(override val selfCost: Long) extends LeafLike {
    override def makeCopy(): LeafLike = copy()
  }

  case class Leaf2(override val selfCost: Long) extends LeafLike {
    override def makeCopy(): LeafLike = copy()
  }

  class LeafToLeaf2 extends RasRule[TestNode] {

    override def shift(node: TestNode): Iterable[TestNode] = {
      node match {
        case Leaf(cost) =>
          List(Leaf2(cost - 1))
        case other => List.empty
      }
    }

    override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
  }

  class UnaryToUnary2 extends RasRule[TestNode] {
    override def shift(node: TestNode): Iterable[TestNode] = {
      node match {
        case Unary(cost, child) =>
          List(Unary2(cost - 1, child))
        case other => List.empty
      }
    }

    override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
  }

  class Unary2ToUnary3 extends RasRule[TestNode] {
    override def shift(node: TestNode): Iterable[TestNode] = {
      node match {
        case Unary2(cost, child) =>
          List(Unary3(cost - 1, child))
        case other => List.empty
      }
    }
    override def shape(): Shape[TestNode] = Shapes.fixedHeight(1)
  }

}
