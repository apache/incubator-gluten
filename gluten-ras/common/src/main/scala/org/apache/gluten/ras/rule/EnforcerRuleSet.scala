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

import org.apache.gluten.ras.Ras
import org.apache.gluten.ras.memo.Closure
import org.apache.gluten.ras.property.PropertySet

import scala.collection.mutable

trait EnforcerRuleSet[T <: AnyRef] {
  def rules(): Seq[RuleApplier[T]]
  def shapes(): Seq[Shape[T]]
}

object EnforcerRuleSet {
  implicit class EnforcerRuleSetImplicits[T <: AnyRef](ruleSet: EnforcerRuleSet[T]) {
    def ++(other: EnforcerRuleSet[T]): EnforcerRuleSet[T] = {
      EnforcerRuleSet(ruleSet.rules() ++ other.rules())
    }
  }

  private def apply[T <: AnyRef](rules: Seq[RuleApplier[T]]): EnforcerRuleSet[T] = {
    new Impl(rules)
  }

  private class Impl[T <: AnyRef](rules: Seq[RuleApplier[T]]) extends EnforcerRuleSet[T] {
    private val ruleShapes: Seq[Shape[T]] = rules.map(_.shape())

    override def rules(): Seq[RuleApplier[T]] = rules
    override def shapes(): Seq[Shape[T]] = ruleShapes
  }

  trait Factory[T <: AnyRef] {
    def ruleSetOf(constraintSet: PropertySet[T]): EnforcerRuleSet[T]
  }

  object Factory {
    def regular[T <: AnyRef](ras: Ras[T], closure: Closure[T]): Factory[T] = {
      new Regular(ras, closure)
    }

    def derive[T <: AnyRef](ras: Ras[T], closure: Closure[T]): Factory[T] = {
      new Derive(ras, closure)
    }

    private class Regular[T <: AnyRef](ras: Ras[T], closure: Closure[T]) extends Factory[T] {
      private val factory = ras.propertySetFactory().newEnforcerRuleFactory()
      private val ruleSetBuffer = mutable.Map[PropertySet[T], EnforcerRuleSet[T]]()

      override def ruleSetOf(constraintSet: PropertySet[T]): EnforcerRuleSet[T] = {
        ruleSetBuffer.getOrElseUpdate(
          constraintSet, {
            val rules =
              factory.newEnforcerRules(constraintSet).map {
                rule: RasRule[T] => RuleApplier.enforcer(ras, closure, constraintSet, rule)
              }
            EnforcerRuleSet(rules)
          }
        )
      }
    }

    private class Derive[T <: AnyRef](ras: Ras[T], closure: Closure[T]) extends Factory[T] {
      import Derive._
      private val ruleSetBuffer = mutable.Map[PropertySet[T], EnforcerRuleSet[T]]()
      override def ruleSetOf(constraintSet: PropertySet[T]): EnforcerRuleSet[T] = {
        val rule = RuleApplier.enforcer(ras, closure, constraintSet, new DeriveEnforcerRule[T]())
        ruleSetBuffer.getOrElseUpdate(constraintSet, EnforcerRuleSet(Seq(rule)))
      }
    }

    private object Derive {
      // A built-in enforcer rule set that does constraint propagation. The rule directly outputs
      // whatever passed in, and memo will copy the output node in with the desired constraint.
      // During witch children constraints will be derived through
      // PropertyDef#getChildrenConstraints. When the children constraints are changed, the
      // new node with changed children constraints will be persisted into the memo.
      private class DeriveEnforcerRule[T <: AnyRef]() extends RasRule[T] {
        override def shift(node: T): Iterable[T] = Seq(node)
        override def shape(): Shape[T] = Shapes.fixedHeight(1)
      }
    }
  }
}
