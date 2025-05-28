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
package org.apache.gluten.ras.property

import org.apache.gluten.ras._
import org.apache.gluten.ras.rule.{EnforcerRuleFactory, RasRule}

import scala.collection.mutable

sealed trait MemoRole[T <: AnyRef] extends Property[T] {
  override def toString: String = this.getClass.getSimpleName
}

object MemoRole {
  implicit class MemoRoleImplicits[T <: AnyRef](role: MemoRole[T]) {
    def asReq(): Req[T] = role.asInstanceOf[Req[T]]
    def asProp(): Prop[T] = role.asInstanceOf[Prop[T]]

    def +:(base: PropertySet[T]): PropertySet[T] = {
      require(!base.asMap.contains(role.definition()))
      val map: Map[PropertyDef[T, _ <: Property[T]], Property[T]] = {
        base.asMap + (role.definition() -> role)
      }
      PropertySet(map)
    }
  }

  trait Req[T <: AnyRef] extends MemoRole[T]
  trait Prop[T <: AnyRef] extends MemoRole[T]

  // Constraints.
  class ReqHub[T <: AnyRef] private[MemoRole] (
      override val definition: PropertyDef[T, _ <: Property[T]])
    extends Req[T]
  class ReqUser[T <: AnyRef] private[MemoRole] (
      override val definition: PropertyDef[T, _ <: Property[T]])
    extends Req[T]
  private class ReqAny[T <: AnyRef] private[MemoRole] (
      override val definition: PropertyDef[T, _ <: Property[T]])
    extends Req[T]

  // Props.
  class Leaf[T <: AnyRef] private[MemoRole] (
      override val definition: PropertyDef[T, _ <: Property[T]])
    extends Prop[T]
  class Hub[T <: AnyRef] private[MemoRole] (
      override val definition: PropertyDef[T, _ <: Property[T]])
    extends Prop[T]
  class User[T <: AnyRef] private[MemoRole] (
      override val definition: PropertyDef[T, _ <: Property[T]])
    extends Prop[T]

  class Def[T <: AnyRef] private[MemoRole] (val planModel: PlanModel[T])
    extends PropertyDef[T, MemoRole[T]] {
    private val groupRoleLookup = mutable.Map[Int, Prop[T]]()

    private val reqAny = new ReqAny[T](this)
    val reqHub = new ReqHub[T](this)
    val reqUser = new ReqUser[T](this)

    val leaf = new Leaf[T](this)
    val hub = new Hub[T](this)
    val user = new User[T](this)

    override def any(): MemoRole[T] = reqAny

    override def getProperty(plan: T): MemoRole[T] = {
      getProperty0(plan)
    }

    private def getProperty0(plan: T): MemoRole[T] = {
      if (planModel.isGroupLeaf(plan)) {
        val groupId = planModel.getGroupId(plan)
        return groupRoleLookup(groupId)
      }
      val children = planModel.childrenOf(plan)
      if (children.isEmpty) {
        return leaf
      }
      val childrenRoles = children.map(getProperty0).distinct
      assert(childrenRoles.size == 1, s"Unidentical children memo roles: $childrenRoles")
      childrenRoles.head
    }

    override def getChildrenConstraints(plan: T, constraint: Property[T]): Seq[MemoRole[T]] = {
      throw new UnsupportedOperationException("Not implemented for MemoRole")
    }

    override def satisfies(property: Property[T], constraint: Property[T]): Boolean =
      (property, constraint) match {
        case (_: Prop[T], _: ReqAny[T]) => true
        case (_: Leaf[T], _: Req[T]) => true
        case (_: User[T], _: ReqUser[T]) => true
        case (_: Hub[T], _: ReqHub[T]) => true
        case _ => false
      }

    override def assignToGroup(
        group: GroupLeafBuilder[T],
        constraint: Property[T]): GroupLeafBuilder[T] = {
      val role: Prop[T] = constraint.asInstanceOf[Req[T]] match {
        case _: ReqAny[T] =>
          hub
        case _: ReqHub[T] =>
          hub
        case _: ReqUser[T] =>
          user
        case _ =>
          throw new IllegalStateException(s"Unexpected req: $constraint")
      }
      groupRoleLookup(group.id()) = role
      group
    }
  }

  implicit class DefImplicits[T <: AnyRef](roleDef: Def[T]) {
    def -:(base: PropertySet[T]): PropertySet[T] = {
      require(base.asMap.contains(roleDef))
      val map: Map[PropertyDef[T, _ <: Property[T]], Property[T]] = {
        base.asMap - roleDef
      }
      PropertySet(map)
    }
  }

  def newDef[T <: AnyRef](planModel: PlanModel[T]): Def[T] = {
    new Def[T](planModel)
  }

  def wrapPropertySetFactory[T <: AnyRef](
      factory: PropertySetFactory[T],
      roleDef: Def[T]): PropertySetFactory[T] = {
    new PropertySetFactoryWithMemoRole[T](factory, roleDef)
  }

  private class PropertySetFactoryWithMemoRole[T <: AnyRef](
      delegate: PropertySetFactory[T],
      roleDef: Def[T])
    extends PropertySetFactory[T] {

    override val any: PropertySet[T] = compose(roleDef.any(), delegate.any())

    override def get(node: T): PropertySet[T] =
      compose(roleDef.getProperty(node), delegate.get(node))

    override def childrenConstraintSets(
        node: T,
        constraintSet: PropertySet[T]): Seq[PropertySet[T]] = {
      assert(!roleDef.planModel.isGroupLeaf(node))

      if (roleDef.planModel.isLeaf(node)) {
        return Nil
      }

      val numChildren = roleDef.planModel.childrenOf(node).size

      def delegateChildrenConstraintSets(): Seq[PropertySet[T]] = {
        val roleRemoved = PropertySet(constraintSet.asMap - roleDef)
        val out = delegate.childrenConstraintSets(node, roleRemoved)
        out
      }

      def delegateConstraintSetAny(): PropertySet[T] = {
        val properties: Seq[Property[T]] = constraintSet.asMap.keys.flatMap {
          case _: Def[T] => Nil
          case other => Seq(other.any())
        }.toSeq
        PropertySet(properties)
      }

      val constraintSets = constraintSet.get(roleDef).asReq() match {
        case _: ReqAny[T] =>
          delegateChildrenConstraintSets().map(
            delegateConstraint => compose(roleDef.any(), delegateConstraint))
        case _: ReqHub[T] =>
          Seq.tabulate(numChildren)(_ => compose(roleDef.reqHub, delegateConstraintSetAny()))
        case _: ReqUser[T] =>
          delegateChildrenConstraintSets().map(
            delegateConstraint => compose(roleDef.reqUser, delegateConstraint))
      }

      constraintSets
    }

    override def assignToGroup(group: GroupLeafBuilder[T], constraintSet: PropertySet[T]): Unit = {
      roleDef.assignToGroup(group, constraintSet.asMap(roleDef))
      delegate.assignToGroup(group, PropertySet(constraintSet.asMap - roleDef))
    }

    override def newEnforcerRuleFactory(): EnforcerRuleFactory[T] = {
      new EnforcerRuleFactory[T] {
        private val delegateFactory: EnforcerRuleFactory[T] = delegate.newEnforcerRuleFactory()

        override def newEnforcerRules(constraintSet: PropertySet[T]): Seq[RasRule[T]] = {
          delegateFactory.newEnforcerRules(constraintSet -: roleDef)
        }
      }
    }

    private def compose(memoRole: MemoRole[T], base: PropertySet[T]): PropertySet[T] = {
      base +: memoRole
    }
  }
}
