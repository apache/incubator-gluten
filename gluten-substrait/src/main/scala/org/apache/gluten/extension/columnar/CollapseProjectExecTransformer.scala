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
package org.apache.gluten.extension.columnar

import org.apache.gluten.GlutenConfig
import org.apache.gluten.execution.ProjectExecTransformer

import org.apache.spark.sql.catalyst.expressions.{Alias, AliasHelper, Attribute, CreateNamedStruct, NamedExpression}
import org.apache.spark.sql.catalyst.optimizer.CollapseProjectShim
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

object CollapseProjectExecTransformer extends Rule[SparkPlan] with AliasHelper {

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!GlutenConfig.getConf.enableColumnarProjectCollapse) {
      return plan
    }
    plan.transformUp {
      case p1 @ ProjectExecTransformer(_, p2: ProjectExecTransformer)
          if canCollapsePreProject(p1, p2)
            && !containsNamedStructAlias(p2.projectList)
            && CollapseProjectShim.canCollapseExpressions(
              p1.projectList,
              p2.projectList,
              alwaysInline = false) =>
        val collapsedProject = p2.copy(projectList =
          CollapseProjectShim.buildCleanedProjectList(p1.projectList, p2.projectList))
        val validationResult = collapsedProject.doValidate()
        if (validationResult.ok()) {
          logDebug(s"Collapse project $p1 and $p2.")
          collapsedProject
        } else {
          logDebug(s"Failed to collapse project, due to ${validationResult.reason()}")
          p1
        }
    }
  }

  /**
   * In Velox, CreateNamedStruct will generate a special output named obj, We cannot collapse such
   * project transformer, otherwise it will result in a bind reference failure.
   */
  private def containsNamedStructAlias(projectList: Seq[NamedExpression]): Boolean = {
    projectList.exists {
      case Alias(_: CreateNamedStruct, _) => true
      case _ => false
    }
  }

  /**
   * We should not collapse the pre-project with its child project, if the Alias(Expression) in
   * pre-project contains other computed results of Expressions in child project. This would lead to
   * the Expression being computed multiple times.
   */
  private def canCollapsePreProject(
      upper: ProjectExecTransformer,
      lower: ProjectExecTransformer): Boolean = {
    !upper.projectList.exists {
      // The logicalLink of pre-project has been set to the logicalLink of its parent node,
      // so it will not be a Project.
      case alias: Alias
        if alias.name.startsWith("_pre_") && upper.logicalLink.exists(!_.isInstanceOf[Project]) =>
        val aliases = getAliasMap(lower.projectList)
        aliases.nonEmpty &&
          alias.collectFirst {
            case a: Attribute => aliases.get(a).exists(!_.child.isInstanceOf[Attribute])
          }.isDefined
      case _ => false
    }
  }
}
