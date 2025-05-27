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

import org.apache.gluten.ras.{GroupLeafBuilder, PlanModel, Property, PropertyDef, PropertyModel}
import org.apache.gluten.ras.rule.EnforcerRuleFactory

import scala.collection.mutable

trait PropertySetFactory[T <: AnyRef] {
  def any(): PropertySet[T]
  def get(node: T): PropertySet[T]
  def childrenConstraintSets(node: T, constraintSet: PropertySet[T]): Seq[PropertySet[T]]
  def assignToGroup(group: GroupLeafBuilder[T], constraintSet: PropertySet[T]): Unit
  def newEnforcerRuleFactory(): EnforcerRuleFactory[T]
}

object PropertySetFactory {
  def apply[T <: AnyRef](
      propertyModel: PropertyModel[T],
      planModel: PlanModel[T]): PropertySetFactory[T] =
    new Impl[T](propertyModel, planModel)

  private class Impl[T <: AnyRef](propertyModel: PropertyModel[T], planModel: PlanModel[T])
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
        propDefs
          .map(
            propDef => {
              val prop = propDef.getProperty(node)
              (propDef, prop)
            })
          .toMap
      val propSet = PropertySet[T](m)
      assert(
        propSet.satisfies(anyConstraint),
        s"Property set $propSet doesn't satisfy its ${'\"'}any${'\"'} variant")
      propSet
    }

    override def childrenConstraintSets(
        node: T,
        constraintSet: PropertySet[T]): Seq[PropertySet[T]] = {
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
            val childrenConstraints = propDef.getChildrenConstraints(node, constraint)
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

    override def assignToGroup(group: GroupLeafBuilder[T], constraintSet: PropertySet[T]): Unit = {
      constraintSet.asMap.foreach {
        case (constraintDef, constraint) =>
          constraintDef.assignToGroup(group, constraint)
      }
    }

    override def newEnforcerRuleFactory(): EnforcerRuleFactory[T] = {
      propertyModel.newEnforcerRuleFactory()
    }
  }
}
