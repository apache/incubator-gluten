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

import org.apache.spark.sql.execution.{ColumnarToRowExec, ColumnarToRowTransition, RowToColumnarExec, RowToColumnarTransition, SparkPlan}

/** Ported from [[ApplyColumnarRulesAndInsertTransitions]] of vanilla Spark. */
object InsertTransitions {

  private def insertRowToColumnar(plan: SparkPlan): SparkPlan = {
    if (!plan.supportsColumnar) {
      // The tree feels kind of backwards
      // Columnar Processing will start here, so transition from row to columnar
      RowToColumnarExec(insertTransitions(plan, outputsColumnar = false))
    } else if (!plan.isInstanceOf[RowToColumnarTransition]) {
      plan.withNewChildren(plan.children.map(insertRowToColumnar))
    } else {
      plan
    }
  }

  def insertTransitions(plan: SparkPlan, outputsColumnar: Boolean): SparkPlan = {
    if (outputsColumnar) {
      insertRowToColumnar(plan)
    } else if (plan.supportsColumnar) {
      // `outputsColumnar` is false but the plan outputs columnar format, so add a
      // to-row transition here.
      ColumnarToRowExec(insertRowToColumnar(plan))
    } else if (!plan.isInstanceOf[ColumnarToRowTransition]) {
      plan.withNewChildren(plan.children.map(insertTransitions(_, outputsColumnar = false)))
    } else {
      plan
    }
  }
}
