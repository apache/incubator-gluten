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
import org.apache.gluten.execution.{ColumnarPartialProjectExec, ProjectExecTransformer, RowToVeloxColumnarExec, VeloxColumnarToRowExec}
import org.apache.gluten.extension.columnar.FallbackTags

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * The rule is to remove the ColumnarPartialProjectExec and replace it with the ProjectExec when the
 * followers of ColumnarPartialProjectExec don't support columnar.
 */
case class RemoveColumnarPartialProject(session: SparkSession) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    if (
      !GlutenConfig.get.enableColumnarPartialProject ||
      !GlutenConfig.get.removeColumnarPartialProject
    ) {
      return plan
    }
    plan.transformDown {
      case VeloxColumnarToRowExec(p: ProjectExecTransformer)
          if p.child.isInstanceOf[ColumnarPartialProjectExec] =>
        val original = p.child.asInstanceOf[ColumnarPartialProjectExec].original
        val projectExecChild = p.child.asInstanceOf[ColumnarPartialProjectExec].child match {
          case RowToVeloxColumnarExec(child) => child // the RowToVeloxColumnarExec is not needed
          case o => o
        }
        val newPlan = if (projectExecChild.supportsColumnar) {
          original.copy(child = VeloxColumnarToRowExec(projectExecChild))
        } else {
          original.copy(child = projectExecChild)
        }
        newPlan.setLogicalLink(original.logicalLink.get)
        // Add fallback tags for the ProjectExec
        FallbackTags.add(
          newPlan,
          ProjectExecTransformer(original.projectList, original.child).doValidate())
        newPlan
    }
  }
}
