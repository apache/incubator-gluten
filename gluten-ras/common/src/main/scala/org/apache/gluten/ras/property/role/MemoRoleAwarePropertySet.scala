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

import org.apache.gluten.ras.{Property, PropertyDef}
import org.apache.gluten.ras.property.PropertySet

sealed private[ras] trait MemoRoleAwarePropertySet[T <: AnyRef] extends PropertySet[T] {
  import MemoRoleAwarePropertySet._
  override def satisfies(other: PropertySet[T]): Boolean = (this, other) match {
    case (p: Prop[T], r: Req[T]) =>
      (p, r) match {
        case (HubPropertySet(), HubConstraintSet()) => true
        case (LeafPropertySet(_), HubConstraintSet()) => true
        case (UserPropertySet(_), HubConstraintSet()) => false
        case (HubPropertySet(), UserConstraintSet(_)) => false
        case (LeafPropertySet(userPropSet), UserConstraintSet(userConstraintSet)) =>
          userPropSet.satisfies(userConstraintSet)
        case (UserPropertySet(userPropSet), UserConstraintSet(userConstraintSet)) =>
          userPropSet.satisfies(userConstraintSet)
      }
    case _ =>
      throw new IllegalStateException(
        "#satisties should only be called with a property set on the LHS " +
          "and a constraint set on the RHS")
  }
}

private[ras] object MemoRoleAwarePropertySet {
  sealed trait Req[T <: AnyRef] extends MemoRoleAwarePropertySet[T]
  sealed trait Prop[T <: AnyRef] extends MemoRoleAwarePropertySet[T]

  case class HubConstraintSet[T <: AnyRef]() extends Req[T] {
    override def get[P <: Property[T]](propertyDef: PropertyDef[T, P]): P = {
      throw new IllegalStateException("Hub constraint set doesn't contain any user constraints")
    }

    override def asMap: Map[PropertyDef[T, _ <: Property[T]], Property[T]] = {
      Map()
    }

    override def toString: String = s"<HUB>"
  }

  case class UserConstraintSet[T <: AnyRef](userConstraintSet: PropertySet[T]) extends Req[T] {
    assert(!userConstraintSet.isInstanceOf[MemoRoleAwarePropertySet[_]])

    override def get[P <: Property[T]](propertyDef: PropertyDef[T, P]): P = {
      userConstraintSet.get(propertyDef)
    }

    override def asMap: Map[PropertyDef[T, _ <: Property[T]], Property[T]] = {
      userConstraintSet.asMap
    }

    override def toString: String = s"<USER>$userConstraintSet"
  }

  case class LeafPropertySet[T <: AnyRef](userPropSet: PropertySet[T]) extends Prop[T] {
    assert(!userPropSet.isInstanceOf[MemoRoleAwarePropertySet[_]])

    override def get[P <: Property[T]](propertyDef: PropertyDef[T, P]): P = {
      userPropSet.get(propertyDef)
    }

    override def asMap: Map[PropertyDef[T, _ <: Property[T]], Property[T]] = {
      userPropSet.asMap
    }

    override def toString: String = s"<LEAF>$userPropSet"
  }

  case class HubPropertySet[T <: AnyRef]() extends Prop[T] {
    override def get[P <: Property[T]](propertyDef: PropertyDef[T, P]): P = {
      throw new IllegalStateException("Hub property set doesn't contain any user properties")
    }

    override def asMap: Map[PropertyDef[T, _ <: Property[T]], Property[T]] = {
      Map()
    }

    override def toString: String = s"<HUB>"
  }

  case class UserPropertySet[T <: AnyRef](userPropSet: PropertySet[T]) extends Prop[T] {
    assert(!userPropSet.isInstanceOf[MemoRoleAwarePropertySet[_]])

    override def get[P <: Property[T]](propertyDef: PropertyDef[T, P]): P = {
      userPropSet.get(propertyDef)
    }

    override def asMap: Map[PropertyDef[T, _ <: Property[T]], Property[T]] = {
      userPropSet.asMap
    }

    override def toString: String = s"<USER>$userPropSet"
  }
}
