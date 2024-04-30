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
package org.apache.gluten.extension.columnar.enumerated

import org.apache.gluten.extension.columnar.validator.Validator
import org.apache.gluten.ras.rule.{RasRule, Shape}

import org.apache.spark.sql.execution.SparkPlan

object ConditionedRule {
  trait PreCondition {
    def apply(node: SparkPlan): Boolean
  }

  object PreCondition {
    implicit class FromValidator(validator: Validator) extends PreCondition {
      override def apply(node: SparkPlan): Boolean = {
        validator.validate(node) match {
          case Validator.Passed => true
          case Validator.Failed(reason) => false
        }
      }
    }
  }

  def wrap(rule: RasRule[SparkPlan], cond: ConditionedRule.PreCondition): RasRule[SparkPlan] = {
    new RasRule[SparkPlan] {
      override def shift(node: SparkPlan): Iterable[SparkPlan] = {
        val out = List(node)
          .filter(cond.apply)
          .flatMap(rule.shift)
        out
      }
      override def shape(): Shape[SparkPlan] = rule.shape()
    }
  }
}
