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

import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.ras.rule.RasRule

import scala.collection.mutable

/**
 * Entrypoint of RAS (relational algebra selector)'s search engine. See basic introduction of RAS:
 * https://github.com/apache/incubator-gluten/issues/5057.
 */
trait Optimization[T <: AnyRef] {
  def newPlanner(
      plan: T,
      constraintSet: PropertySet[T],
      altConstraintSets: Seq[PropertySet[T]]): RasPlanner[T]

  def propSetOf(plan: T): PropertySet[T]

  def withNewConfig(confFunc: RasConfig => RasConfig): Optimization[T]
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

  implicit class OptimizationImplicits[T <: AnyRef](opt: Optimization[T]) {
    def newPlanner(plan: T): RasPlanner[T] = {
      opt.newPlanner(plan, opt.propSetOf(plan), List.empty)
    }
    def newPlanner(plan: T, constraintSet: PropertySet[T]): RasPlanner[T] = {
      opt.newPlanner(plan, constraintSet, List.empty)
    }
  }
}

class Ras[T <: AnyRef] private (
    val config: RasConfig,
    val planModel: PlanModel[T],
    val costModel: CostModel[T],
    val metadataModel: MetadataModel[T],
    val propertyModel: PropertyModel[T],
    val explain: RasExplain[T],
    val ruleFactory: RasRule.Factory[T])
  extends Optimization[T] {
  import Ras._

  override def withNewConfig(confFunc: RasConfig => RasConfig): Ras[T] = {
    new Ras(
      confFunc(config),
      planModel,
      costModel,
      metadataModel,
      propertyModel,
      explain,
      ruleFactory)
  }

  private val propSetFactory: PropertySetFactory[T] = PropertySetFactory(propertyModel, planModel)
  // Normal groups start with ID 0, so it's safe to use Int.MinValue to do validation.
  private val dummyGroup: T =
    planModel.newGroupLeaf(Int.MinValue, metadataModel.dummy(), propSetFactory.any())
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
    propertyModel.propertyDefs.foreach {
      propDef =>
        // Node groups don't have user-defined property, expect exception here.
        assertThrows(
          "Group is not allowed to return its property directly to optimizer (optimizer already" +
            " knew that). It's expected to throw an exception when getting its property but not") {
          propDef.getProperty(dummyGroup)
        }
    }
  }

  override def newPlanner(
      plan: T,
      constraintSet: PropertySet[T],
      altConstraintSets: Seq[PropertySet[T]]): RasPlanner[T] = {
    RasPlanner(this, altConstraintSets, constraintSet, plan)
  }

  override def propSetOf(plan: T): PropertySet[T] = propertySetFactory().get(plan)

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
    planModel.childrenOf(node).isEmpty
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

  private[ras] def propertySetFactory(): PropertySetFactory[T] = propSetFactory

  private[ras] def dummyGroupLeaf(): T = {
    dummyGroup
  }

  private[ras] def getInfCost(): Cost = infCost

  private[ras] def isInfCost(cost: Cost) = costModel.costComparator().equiv(cost, infCost)

  private[ras] def toUnsafeKey(node: T): UnsafeKey[T] = UnsafeKey(this, node)
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

  trait PropertySetFactory[T <: AnyRef] {
    def any(): PropertySet[T]
    def get(node: T): PropertySet[T]
    def childrenConstraintSets(constraintSet: PropertySet[T], node: T): Seq[PropertySet[T]]
  }

  private object PropertySetFactory {
    def apply[T <: AnyRef](
        propertyModel: PropertyModel[T],
        planModel: PlanModel[T]): PropertySetFactory[T] =
      new PropertySetFactoryImpl[T](propertyModel, planModel)

    private class PropertySetFactoryImpl[T <: AnyRef](
        propertyModel: PropertyModel[T],
        planModel: PlanModel[T])
      extends PropertySetFactory[T] {
      private val propDefs: Seq[PropertyDef[T, _ <: Property[T]]] = propertyModel.propertyDefs
      private val anyConstraint = {
        val m: Map[PropertyDef[T, _ <: Property[T]], Property[T]] =
          propDefs.map(propDef => (propDef, propDef.any())).toMap
        PropertySet[T](m)
      }

      override def any(): PropertySet[T] = anyConstraint

      override def get(node: T): PropertySet[T] = {
        val m: Map[PropertyDef[T, _ <: Property[T]], Property[T]] =
          propDefs.map(propDef => (propDef, propDef.getProperty(node))).toMap
        PropertySet[T](m)
      }

      override def childrenConstraintSets(
          constraintSet: PropertySet[T],
          node: T): Seq[PropertySet[T]] = {
        val builder: Seq[mutable.Map[PropertyDef[T, _ <: Property[T]], Property[T]]] =
          planModel
            .childrenOf(node)
            .map(_ => mutable.Map[PropertyDef[T, _ <: Property[T]], Property[T]]())

        propDefs
          .foldLeft(builder) {
            (
                builder: Seq[mutable.Map[PropertyDef[T, _ <: Property[T]], Property[T]]],
                propDef: PropertyDef[T, _ <: Property[T]]) =>
              val constraint = constraintSet.get(propDef)
              val childrenConstraints = propDef.getChildrenConstraints(constraint, node)
              builder.zip(childrenConstraints).map {
                case (childBuilder, childConstraint) =>
                  childBuilder += (propDef -> childConstraint)
              }
          }
          .map {
            builder: mutable.Map[PropertyDef[T, _ <: Property[T]], Property[T]] =>
              PropertySet[T](builder.toMap)
          }
      }
    }
  }

  trait UnsafeKey[T]

  private object UnsafeKey {
    def apply[T <: AnyRef](ras: Ras[T], self: T): UnsafeKey[T] = new UnsafeKeyImpl(ras, self)
    private class UnsafeKeyImpl[T <: AnyRef](ras: Ras[T], val self: T) extends UnsafeKey[T] {
      override def hashCode(): Int = ras.planModel.hashCode(self)
      override def equals(other: Any): Boolean = {
        other match {
          case that: UnsafeKeyImpl[T] => ras.planModel.equals(self, that.self)
          case _ => false
        }
      }
      override def toString: String = ras.explain.describeNode(self)
    }
  }
}
