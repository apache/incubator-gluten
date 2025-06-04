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

import org.apache.gluten.ras.rule.EnforcerRuleFactory

// TODO Use class tags to restrict runtime user-defined class types.

trait Property[T <: AnyRef] {
  def definition(): PropertyDef[T, _ <: Property[T]]
}

object Property {
  implicit class PropertyImplicits[T <: AnyRef](property: Property[T]) {
    def satisfies(constraint: Property[T]): Boolean = {
      property.definition().satisfies(property, constraint)
    }
  }
}

trait PropertyDef[T <: AnyRef, P <: Property[T]] {
  def any(): P
  def getProperty(plan: T): P
  def getChildrenConstraints(plan: T, constraint: Property[T]): Seq[P]
  def satisfies(property: Property[T], constraint: Property[T]): Boolean
  def assignToGroup(group: GroupLeafBuilder[T], constraint: Property[T]): GroupLeafBuilder[T]
}

trait PropertyModel[T <: AnyRef] {
  def propertyDefs: Seq[PropertyDef[T, _ <: Property[T]]]
  def newEnforcerRuleFactory(): EnforcerRuleFactory[T]
}

object PropertyModel {}
