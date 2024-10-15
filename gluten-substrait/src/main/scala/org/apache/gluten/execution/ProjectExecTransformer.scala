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
package org.apache.gluten.execution

import org.apache.gluten.backendsapi.BackendsApiManager

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.execution.SparkPlan

case class ProjectExecTransformer(projectList: Seq[NamedExpression], child: SparkPlan)
  extends ProjectExecTransformerBase(projectList, child) {

  override protected def withNewChildInternal(newChild: SparkPlan): ProjectExecTransformer =
    copy(child = newChild)
}

object ProjectExecTransformer {

  def apply(projectList: Seq[NamedExpression], child: SparkPlan): ProjectExecTransformer = {
    BackendsApiManager.getSparkPlanExecApiInstance.genProjectExecTransformer(projectList, child)
  }

  // Directly creating a project transformer may not be considered safe since some backends, E.g.,
  // Clickhouse may require to intercept the instantiation procedure.
  def createUnsafe(projectList: Seq[NamedExpression], child: SparkPlan): ProjectExecTransformer =
    new ProjectExecTransformer(projectList, child)
}
