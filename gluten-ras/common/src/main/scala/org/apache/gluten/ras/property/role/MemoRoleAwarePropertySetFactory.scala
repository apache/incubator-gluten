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
package org.apache.gluten.ras.property.role

import org.apache.gluten.ras.{GroupLeafBuilder, PlanModel}
import org.apache.gluten.ras.property.{PropertySet, PropertySetFactory}
import org.apache.gluten.ras.rule.{EnforcerRuleFactory, RasRule}

import scala.collection.mutable

private[property] class MemoRoleAwarePropertySetFactory[T <: AnyRef](
    val planModel: PlanModel[T],
    val userPropSetFactory: PropertySetFactory[T])
  extends PropertySetFactory[T] {
  assert(!userPropSetFactory.isInstanceOf[MemoRoleAwarePropertySetFactory[_]])

  private val groupRoleLookup = mutable.Map[Int, MemoRole]()

  override def any(): PropertySet[T] = {
    MemoRoleAwarePropertySet.UserConstraintSet(userPropSetFactory.any())
  }

  override def get(node: T): PropertySet[T] = {
    val role = getMemoRole(node)
    role match {
      case MemoRole.Hub =>
        MemoRoleAwarePropertySet.HubPropertySet()
      case MemoRole.User =>
        MemoRoleAwarePropertySet.UserPropertySet(userPropSetFactory.get(node))
      case MemoRole.Leaf =>
        MemoRoleAwarePropertySet.LeafPropertySet(userPropSetFactory.get(node))
    }
  }

  private def getMemoRole(node: T): MemoRole = {
    if (planModel.isGroupLeaf(node)) {
      val groupId = planModel.getGroupId(node)
      return groupRoleLookup(groupId)
    }
    if (planModel.isLeaf(node)) {
      return MemoRole.Leaf
    }
    val children = planModel.childrenOf(node)
    val childrenRoles = children.map(getMemoRole).distinct
    assert(childrenRoles.size == 1, s"Non-identical children memo roles: $childrenRoles")
    childrenRoles.head
  }

  override def childrenConstraintSets(
      node: T,
      constraintSet: PropertySet[T]): Seq[PropertySet[T]] = {
    assert(!planModel.isGroupLeaf(node))

    if (planModel.isLeaf(node)) {
      return Nil
    }

    val numChildren = planModel.childrenOf(node).size

    constraintSet match {
      case MemoRoleAwarePropertySet.HubConstraintSet() =>
        Seq.tabulate(numChildren)(_ => MemoRoleAwarePropertySet.HubConstraintSet())
      case MemoRoleAwarePropertySet.UserConstraintSet(userConstraintSet) =>
        userPropSetFactory
          .childrenConstraintSets(node, userConstraintSet)
          .map(userSet => MemoRoleAwarePropertySet.UserConstraintSet(userSet))
    }
  }

  override def assignToGroup(group: GroupLeafBuilder[T], constraintSet: PropertySet[T]): Unit = {
    constraintSet match {
      case MemoRoleAwarePropertySet.HubConstraintSet() =>
        groupRoleLookup(group.id()) = MemoRole.Hub
        userPropSetFactory.assignToGroup(group, userPropSetFactory.any())
      case MemoRoleAwarePropertySet.UserConstraintSet(userConstraintSet) =>
        groupRoleLookup(group.id()) = MemoRole.User
        userPropSetFactory.assignToGroup(group, userConstraintSet)
    }
  }

  override def newEnforcerRuleFactory(): EnforcerRuleFactory[T] = {
    new EnforcerRuleFactory[T] {
      private val userEnforcerRuleFactory = userPropSetFactory.newEnforcerRuleFactory()

      override def newEnforcerRules(constraintSet: PropertySet[T]): Seq[RasRule[T]] = {
        constraintSet match {
          case MemoRoleAwarePropertySet.HubConstraintSet() =>
            Nil
          case MemoRoleAwarePropertySet.UserConstraintSet(userConstraintSet) =>
            userEnforcerRuleFactory.newEnforcerRules(userConstraintSet)
        }
      }
    }
  }
}
