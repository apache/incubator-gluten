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
package org.apache.gluten.extension

import org.apache.gluten.execution.CHHashAggregateExecTransformer

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

case class DistinctAggregateRule() extends Rule[SparkPlan] {

  private var hasDistinct = false

  override def apply(plan: SparkPlan): SparkPlan = {
    val newPlan = plan.transform { case p => setDistinctTag(p) }
    newPlan
  }

  private def setDistinctTag(plan: SparkPlan): SparkPlan = {
    plan match {
      case p: CHHashAggregateExecTransformer =>
        p.aggregateExpressions.foreach(
          x => {
            if (x.isDistinct) {
              hasDistinct = true
            }
          })
        if (hasDistinct) {
          val newPlan = p.copy(hasDistinctAggregate = true)
          return newPlan
        }
      case _ =>
    }
    plan
  }

}
