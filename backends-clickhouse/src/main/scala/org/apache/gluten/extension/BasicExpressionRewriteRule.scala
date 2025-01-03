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

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/*
 * This file includes some rules to repace expressions in more efficient way.
 */

// Try to replace `from_json` with `get_json_object` if possible.
class RepalceFromJsonWithGetJsonObject(spark: SparkSession) extends Rule[LogicalPlan] with Logging {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!CHBackendSettings.enableReplaceFromJsonWithGetJsonObject || !plan.resolved) {
      plan;
    } else {
      visitPlan(plan)
    }
  }

  def visitPlan(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case project: Project =>
        val newProjectList = project.projectList.map(expr => visitNamedExpression(expr))
        Project(newProjectList, project.child)
      case other =>
        other.withNewChildren(other.children.map(visitPlan))
    }
  }

  def visitNamedExpression(namedExpr: NamedExpression): NamedExpression = {
    val expr = namedExpr.asInstanceOf[Expression]
    expr.withNewChildren(expr.children.map(visitExpression)).asInstanceOf[NamedExpression]
  }

  def visitExpression(expr: Expression): Expression = {
    expr match {
      case getMapValue: GetMapValue
          if getMapValue.child.isInstanceOf[JsonToStructs] &&
            getMapValue.child.dataType.isInstanceOf[MapType] &&
            getMapValue.child.dataType.asInstanceOf[MapType].valueType.isInstanceOf[StringType] &&
            getMapValue.key.isInstanceOf[Literal] &&
            getMapValue.key.dataType.isInstanceOf[StringType] =>
        val child = visitExpression(getMapValue.child.asInstanceOf[JsonToStructs].child)
        val key = UTF8String.fromString(s"$$.${getMapValue.key.asInstanceOf[Literal].value}")
        GetJsonObject(child, Literal(key, StringType))
      case other =>
        other.withNewChildren(other.children.map(visitExpression))
    }
  }
}
