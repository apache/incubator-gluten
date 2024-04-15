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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.columnar.TransformHints
import org.apache.gluten.ras.rule.{RasRule, Shape, Shapes}

import org.apache.spark.sql.execution.{FilterExec, SparkPlan}

object ImplementFilter extends RasRule[SparkPlan] {
  override def shift(node: SparkPlan): Iterable[SparkPlan] = node match {
    case plan if TransformHints.isNotTransformable(plan) => List.empty
    case FilterExec(condition, child) =>
      List(
        BackendsApiManager.getSparkPlanExecApiInstance
          .genFilterExecTransformer(condition, child))
    case _ =>
      List.empty
  }
  override def shape(): Shape[SparkPlan] = Shapes.fixedHeight(1)
}
