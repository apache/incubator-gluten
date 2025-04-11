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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.{ColumnarPartialProjectExec, RowToVeloxColumnarExec, VeloxColumnarToRowExec}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}

case class PartialProjectRule(spark: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.get.enableColumnarPartialProject) {
      return plan
    }
    plan.transformDown {
      // case 1: This case handles the scenario where both the child and parent
      // of ProjectExec are Columnar.
      // RowToColumnar
      //   ProjectExec
      //     ColumnarToRow
      case plan @ RowToVeloxColumnarExec(p @ ProjectExec(_, VeloxColumnarToRowExec(child))) =>
        val transformer = ColumnarPartialProjectExec.create(p.copy(child = child))
        if (
          transformer.doValidate().ok() &&
          transformer.child.asInstanceOf[ColumnarPartialProjectExec].doValidate().ok()
        ) {
          transformer
        } else plan
      // case 2: This case handles the scenario where a ProjectExec's child is
      // a VeloxColumnarToRowExec.
      //   ProjectExec
      //     ColumnarToRow
      case plan @ ProjectExec(_, VeloxColumnarToRowExec(child)) =>
        val transformer = ColumnarPartialProjectExec.create(ProjectExec(plan.projectList, child))
        if (
          transformer.doValidate().ok() &&
          transformer.child.asInstanceOf[ColumnarPartialProjectExec].doValidate().ok()
        ) {
          VeloxColumnarToRowExec(
            transformer
          ) // ensure the output is Row, which is the same as ProjectExec
        } else plan
      // case 3:
      //   ProjectExec
      //     row
      // case p: ProjectExec => p // we don't need to do anything for this case
    }
  }
}
