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
package org.apache.gluten.ras.rule

import org.apache.gluten.ras.{Property, PropertyDef}
import org.apache.gluten.ras.property.PropertySet

trait EnforcerRuleFactory[T <: AnyRef] {
  def newEnforcerRules(constraintSet: PropertySet[T]): Seq[RasRule[T]]
}

object EnforcerRuleFactory {
  def fromSubRules[T <: AnyRef](
      subRuleFactories: Seq[SubRuleFactory[T]]): EnforcerRuleFactory[T] = {
    new FromSubRules[T](subRuleFactories)
  }

  trait SubRule[T <: AnyRef] {
    def enforce(node: T, constraint: Property[T]): Iterable[T]
  }

  trait SubRuleFactory[T <: AnyRef] {
    def newSubRule(constraintDef: PropertyDef[T, _ <: Property[T]]): SubRule[T]
    def ruleShape: Shape[T]
  }

  private class FromSubRules[T <: AnyRef](subRuleFactories: Seq[SubRuleFactory[T]])
    extends EnforcerRuleFactory[T] {
    override def newEnforcerRules(constraintSet: PropertySet[T]): Seq[RasRule[T]] = {
      subRuleFactories.map {
        subRuleFactory =>
          new RasRule[T] {
            override def shift(node: T): Iterable[T] = {
              val out = constraintSet.asMap
                .scanLeft(Seq(node)) {
                  case (nodes, (constraintDef, constraint)) =>
                    val subRule = subRuleFactory.newSubRule(constraintDef)
                    val intermediate = nodes.flatMap(
                      n => {
                        val after = subRule.enforce(n, constraint)
                        after
                      })
                    intermediate
                }
                .flatten
              out
            }

            override def shape(): Shape[T] = subRuleFactory.ruleShape
          }
      }
    }
  }
}
