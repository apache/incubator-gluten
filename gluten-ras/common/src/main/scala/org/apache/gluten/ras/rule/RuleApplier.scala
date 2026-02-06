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

import org.apache.gluten.ras._
import org.apache.gluten.ras.Ras.UnsafeHashKey
import org.apache.gluten.ras.memo.Closure
import org.apache.gluten.ras.path.InClusterPath
import org.apache.gluten.ras.property.PropertySet

import java.util

import scala.collection.mutable

trait RuleApplier[T <: AnyRef] {
  def apply(icp: InClusterPath[T]): Unit
  def shape(): Shape[T]
}

object RuleApplier {
  def regular[T <: AnyRef](ras: Ras[T], closure: Closure[T], rule: RasRule[T]): RuleApplier[T] = {
    new RegularRuleApplier(ras, closure, rule)
  }

  def enforcer[T <: AnyRef](
      ras: Ras[T],
      closure: Closure[T],
      constraintSet: PropertySet[T],
      rule: RasRule[T]): RuleApplier[T] = {
    new EnforcerRuleApplier[T](ras, closure, constraintSet, rule)
  }

  private class RegularRuleApplier[T <: AnyRef](ras: Ras[T], closure: Closure[T], rule: RasRule[T])
    extends RuleApplier[T] {
    private val deDup = DeDup(ras)

    override def apply(icp: InClusterPath[T]): Unit = {
      if (!shape.identify(icp.path())) {
        return
      }
      val cKey = icp.cluster()
      val path = icp.path()
      val plan = path.plan()
      deDup.run(cKey, plan) {
        apply0(cKey, plan)
      }
    }

    private def apply0(cKey: RasClusterKey, plan: T): Unit = {
      val equivalents = rule.shift(plan)
      equivalents.foreach {
        equiv =>
          closure
            .openFor(cKey)
            .memorize(equiv, ras.userConstraintSet())
      }
    }

    override val shape: Shape[T] = rule.shape()
  }

  private class EnforcerRuleApplier[T <: AnyRef](
      ras: Ras[T],
      closure: Closure[T],
      constraintSet: PropertySet[T],
      rule: RasRule[T])
    extends RuleApplier[T] {
    private val deDup = DeDup(ras)

    override def apply(icp: InClusterPath[T]): Unit = {
      if (!shape.identify(icp.path())) {
        return
      }
      val cKey = icp.cluster()
      val path = icp.path()
      val propSet = path.node().self().propSet()
      if (propSet.satisfies(constraintSet)) {
        return
      }
      val plan = path.plan()
      deDup.run(cKey, plan) {
        apply0(cKey, constraintSet, plan)
      }
    }

    private def apply0(cKey: RasClusterKey, constraintSet: PropertySet[T], plan: T): Unit = {
      val equivalents = rule.shift(plan)
      equivalents.foreach {
        equiv =>
          closure
            .openFor(cKey)
            .memorize(equiv, constraintSet)
      }
    }

    override val shape: Shape[T] = rule.shape()
  }

  private trait DeDup[T <: AnyRef] {
    def run(cKey: RasClusterKey, plan: T)(computation: => Unit): Unit
  }

  private object DeDup {
    def apply[T <: AnyRef](ras: Ras[T]): DeDup[T] = {
      new Impl[T](ras)
    }

    private class Impl[T <: AnyRef](ras: Ras[T]) extends DeDup[T] {
      private val layerOne = mutable.Map[RasClusterKey, java.util.IdentityHashMap[T, Object]]()
      private val layerTwo = mutable.Map[RasClusterKey, mutable.Set[UnsafeHashKey[T]]]()

      override def run(cKey: RasClusterKey, plan: T)(computation: => Unit): Unit = {
        // L1 cache is built on the identity hash codes of the input query plans. If
        // the cache is hit, which means the same plan object in this JVM was
        // once applied for the computation. Return fast in that case.
        val l1Plans = layerOne.getOrElseUpdate(cKey, new util.IdentityHashMap())
        if (l1Plans.containsKey(plan)) {
          // The L1 cache is hit.
          return
        }
        // Add the plan object into L1 cache.
        l1Plans.put(plan, new Object)

        // L2 cache is built on the equalities of the input query plans. It internally
        // compares plans through RAS API PlanMode#equals. If the cache is hit, which
        // means an identical plan (but not necessarily the same one) was once applied
        // for the computation. Return fast in that case.
        val l2Plans = layerTwo.getOrElseUpdate(cKey, mutable.Set())
        val pKey = ras.toHashKey(plan)
        if (l2Plans.contains(pKey)) {
          // The L2 cache is hit.
          return
        }
        // Add the plan object into L2 cache.
        l2Plans += pKey

        // All cache missed, apply the computation on the plan.
        computation
      }
    }
  }
}
