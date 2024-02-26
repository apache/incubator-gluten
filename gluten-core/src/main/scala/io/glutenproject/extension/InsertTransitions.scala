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
package io.glutenproject.extension

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ApplyColumnarRulesAndInsertTransitions, SparkPlan}

/** See rule code from vanilla Spark: [[ApplyColumnarRulesAndInsertTransitions]]. */
case class InsertTransitions(outputsColumnar: Boolean) extends Rule[SparkPlan] {
  private val rule = ApplyColumnarRulesAndInsertTransitions(List(), outputsColumnar)
  override def apply(plan: SparkPlan): SparkPlan = rule.apply(plan)
}

object InsertTransitions {
  def insertTransitions(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan = {
    InsertTransitions(outputsColumnar).apply(plan)
  }
}
