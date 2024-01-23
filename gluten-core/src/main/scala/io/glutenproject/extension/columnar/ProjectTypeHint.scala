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
package io.glutenproject.extension.columnar

import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.SparkPlan

trait ProjectTypeHint
case class PRE_PROJECT() extends ProjectTypeHint

object ProjectTypeHint {
  val TAG: TreeNodeTag[ProjectTypeHint] =
    TreeNodeTag[ProjectTypeHint]("io.glutenproject.projecttypehint")

  def isAlreadyTagged(plan: SparkPlan): Boolean = {
    plan.getTagValue(TAG).isDefined
  }

  def isPreProject(plan: SparkPlan): Boolean = {
    plan
      .getTagValue(TAG)
      .isDefined && plan.getTagValue(TAG).get.isInstanceOf[PRE_PROJECT]
  }

  /**
   * Tag a project plan as pre-project, pre-project is pulled out by the ColumnarPullOutPreProject
   * rule for computing the expressions contained in the SparkPlan in advance. In some cases, we
   * need to perform certain logic based on whether the project is a pre-project. For example, the
   * SortExecTransformer's outputOrdering needs to be adjusted when its child is found to be a
   * pre-project.
   */
  def tagPreProject(plan: SparkPlan): Unit = {
    tag(plan, PRE_PROJECT())
  }

  private def tag(plan: SparkPlan, hint: ProjectTypeHint): Unit = {
    plan.setTagValue(TAG, hint)
  }
}
