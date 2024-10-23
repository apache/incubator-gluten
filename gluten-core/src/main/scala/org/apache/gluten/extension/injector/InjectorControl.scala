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
package org.apache.gluten.extension.injector

import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable

private class InjectorControl() {
  import InjectorControl._
  private val logicalRuleDisablerBuffer: mutable.ListBuffer[Disabler[LogicalPlan]] =
    mutable.ListBuffer()
  private val physicalRuleDisablerBuffer: mutable.ListBuffer[Disabler[SparkPlan]] =
    mutable.ListBuffer()

  def disableLogicalRulesOn(disabler: Disabler[LogicalPlan]): Unit = synchronized {
    logicalRuleDisablerBuffer += disabler
  }

  def disablePhysicalRulesOn(disabler: Disabler[SparkPlan]): Unit = synchronized {
    physicalRuleDisablerBuffer += disabler
  }

  private[injector] def logicalRuleWithDisabler(
      session: SparkSession,
      rule: Rule[LogicalPlan]): Rule[LogicalPlan] =
    synchronized {
      logicalRuleDisablerBuffer.foldLeft(rule) { case (r, d) => d.wrapLogicalRule(session, r) }
    }

  private[injector] def physicalRuleWithDisabler(
      session: SparkSession,
      rule: Rule[SparkPlan]): Rule[SparkPlan] =
    synchronized {
      physicalRuleDisablerBuffer.foldLeft(rule) { case (r, d) => d.wrapPhysicalRule(session, r) }
    }

  private[injector] def strategyWithDisabler(session: SparkSession, rule: Strategy): Strategy =
    synchronized {
      logicalRuleDisablerBuffer.foldLeft(rule) { case (r, d) => d.wrapStrategy(session, r) }
    }
}

object InjectorControl {
  trait Disabler[T <: QueryPlan[_]] {
    // If true, the injected rule will be disabled.
    def disabledFor(session: SparkSession, plan: T): Boolean
  }

  implicit private class LogicalDisablerOps(d: Disabler[LogicalPlan]) {
    def wrapLogicalRule(session: SparkSession, rule: Rule[LogicalPlan]): Rule[LogicalPlan] = {
      new Rule[LogicalPlan] {
        override val ruleName: String = rule.ruleName
        override def apply(plan: LogicalPlan): LogicalPlan = {
          if (d.disabledFor(session, plan)) {
            return plan
          }
          rule(plan)
        }

      }
    }

    def wrapStrategy(session: SparkSession, rule: Strategy): Strategy = {
      new Strategy {
        override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
          if (d.disabledFor(session, plan)) {
            return Nil
          }
          rule(plan)
        }
      }
    }
  }

  implicit private class PhysicalDisablerOps(d: Disabler[SparkPlan]) {
    def wrapPhysicalRule(session: SparkSession, rule: Rule[SparkPlan]): Rule[SparkPlan] = {
      new Rule[SparkPlan] {
        override val ruleName: String = rule.ruleName

        override def apply(plan: SparkPlan): SparkPlan = {
          if (d.disabledFor(session, plan)) {
            return plan
          }
          rule(plan)
        }
      }
    }
  }
}
