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

import org.apache.gluten.ras.property.{MemoRole, PropertySet, PropertySetFactory}
import org.apache.gluten.ras.property.MemoRole.PropertySetFactoryWithMemoRole
import org.apache.gluten.ras.rule.{EnforcerRuleFactory, RasRule}

/**
 * Entrypoint of RAS (relational algebra selector) 's search engine. See the basic introduction of
 * RAS: https://github.com/apache/incubator-gluten/issues/5057.
 */
trait Optimization[T <: AnyRef] {
  def newPlanner(plan: T, constraintSet: PropertySet[T]): RasPlanner[T]
}

object Optimization {
  def apply[T <: AnyRef](
      planModel: PlanModel[T],
      costModel: CostModel[T],
      metadataModel: MetadataModel[T],
      propertyModel: PropertyModel[T],
      explain: RasExplain[T],
      ruleFactory: RasRule.Factory[T]): Optimization[T] = {
    Ras(planModel, costModel, metadataModel, propertyModel, explain, ruleFactory)
  }
}

class Ras[T <: AnyRef] private (
    val config: RasConfig,
    val planModel: PlanModel[T],
    val costModel: CostModel[T],
    val metadataModel: MetadataModel[T],
    private val propertyModel: PropertyModel[T],
    val explain: RasExplain[T],
    val ruleFactory: RasRule.Factory[T])
  extends Optimization[T] {
  import Ras._

  private val propSetFactory: PropertySetFactoryWithMemoRole[T] = {
    val memoRoleDef: MemoRole.Def[T] = MemoRole.newDef(planModel)
    val baseFactory = PropertySetFactory(propertyModel, planModel)
    MemoRole.wrapPropertySetFactory(baseFactory, memoRoleDef)
  }
  // Normal groups start with ID 0, so it's safe to use Int.MinValue to do validation.
  private val dummyGroup: T =
    newGroupLeaf(Int.MinValue, metadataModel.dummy(), propSetFactory.any())
  private val infCost: Cost = costModel.makeInfCost()

  validateModels()

  private def assertThrows(message: String)(u: => Unit): Unit = {
    var notThrew: Boolean = false
    try {
      u
      notThrew = true
    } catch {
      case _: Exception =>
    }
    assert(!notThrew, message)
  }

  private def validateModels(): Unit = {
    // Node groups are leafs.
    assert(planModel.childrenOf(dummyGroup) == List.empty)
    assertThrows(
      "Group is not allowed to have cost. It's expected to throw an exception when " +
        "getting its cost but not") {
      // Node groups don't have user-defined cost, expect exception here.
      costModel.costOf(dummyGroup)
    }
    assertThrows(
      "Group is not allowed to return its metadata directly to optimizer (optimizer already" +
        " knew that). It's expected to throw an exception when getting its metadata but not") {
      // Node groups don't have user-defined cost, expect exception here.
      metadataModel.metadataOf(dummyGroup)
    }
  }

  override def newPlanner(plan: T, constraintSet: PropertySet[T]): RasPlanner[T] = {
    RasPlanner(this, withUserConstraint(constraintSet), plan)
  }

  def newPlanner(plan: T): RasPlanner[T] = {
    RasPlanner(this, userConstraintSet(), plan)
  }

  def withNewConfig(confFunc: RasConfig => RasConfig): Ras[T] = {
    new Ras(
      confFunc(config),
      planModel,
      costModel,
      metadataModel,
      propertyModel,
      explain,
      ruleFactory)
  }

  private[ras] def withUserConstraint(from: PropertySet[T]): PropertySet[T] = {
    from +: propSetFactory.userConstraint()
  }

  private[ras] def userConstraintSet(): PropertySet[T] = propSetFactory.userConstraintSet()

  private[ras] def hubConstraintSet(): PropertySet[T] = propSetFactory.hubConstraintSet()

  private[ras] def propSetOf(plan: T): PropertySet[T] = {
    propSetFactory.get(plan)
  }

  private[ras] def childrenConstraintSets(
      node: T,
      constraintSet: PropertySet[T]): Seq[PropertySet[T]] = {
    propSetFactory.childrenConstraintSets(node, constraintSet)
  }

  private[ras] def newEnforcerRuleFactory(): EnforcerRuleFactory[T] = {
    propSetFactory.newEnforcerRuleFactory()
  }

  private[ras] def withNewChildren(node: T, newChildren: Seq[T]): T = {
    val oldChildren = planModel.childrenOf(node)
    assert(newChildren.size == oldChildren.size)
    val out = planModel.withNewChildren(node, newChildren)
    assert(planModel.childrenOf(out).size == newChildren.size)
    out
  }

  private[ras] def isGroupLeaf(node: T): Boolean = {
    planModel.isGroupLeaf(node)
  }

  private[ras] def isLeaf(node: T): Boolean = {
    planModel.isLeaf(node)
  }

  private[ras] def isCanonical(node: T): Boolean = {
    assert(!planModel.isGroupLeaf(node))
    planModel.childrenOf(node).forall(child => planModel.isGroupLeaf(child))
  }

  private[ras] def getChildrenGroupIds(n: T): Seq[Int] = {
    assert(isCanonical(n))
    planModel
      .childrenOf(n)
      .map(child => planModel.getGroupId(child))
  }

  private[ras] def dummyGroupLeaf(): T = {
    dummyGroup
  }

  private[ras] def getInfCost(): Cost = infCost

  private[ras] def isInfCost(cost: Cost) = costModel.costComparator().equiv(cost, infCost)

  private[ras] def toHashKey(node: T): UnsafeHashKey[T] = UnsafeHashKey(this, node)

  private[ras] def newGroupLeaf(groupId: Int, meta: Metadata, constraintSet: PropertySet[T]): T = {
    val builder = planModel.newGroupLeaf(groupId)
    metadataModel.assignToGroup(builder, meta)
    propSetFactory.assignToGroup(builder, constraintSet)
    builder.build()
  }
}

object Ras {
  private[ras] def apply[T <: AnyRef](
      planModel: PlanModel[T],
      costModel: CostModel[T],
      metadataModel: MetadataModel[T],
      propertyModel: PropertyModel[T],
      explain: RasExplain[T],
      ruleFactory: RasRule.Factory[T]): Ras[T] = {
    new Ras[T](
      RasConfig(),
      planModel,
      costModel,
      metadataModel,
      propertyModel,
      explain,
      ruleFactory)
  }

  trait UnsafeHashKey[T]

  private object UnsafeHashKey {
    def apply[T <: AnyRef](ras: Ras[T], self: T): UnsafeHashKey[T] =
      new UnsafeHashKeyImpl(ras, self)
    private class UnsafeHashKeyImpl[T <: AnyRef](ras: Ras[T], val self: T)
      extends UnsafeHashKey[T] {
      override def hashCode(): Int = ras.planModel.hashCode(self)
      override def equals(other: Any): Boolean = {
        other match {
          case that: UnsafeHashKeyImpl[T] => ras.planModel.equals(self, that.self)
          case _ => false
        }
      }
      override def toString: String = ras.explain.describeNode(self)
    }
  }
}
