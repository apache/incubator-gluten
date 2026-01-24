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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.{FilterExecTransformerBase, ProjectExecTransformer}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * SparkPlan-level rewrite for pattern `array_contains(map_keys(m), k)` to `map_contains_key(m, k)`.
 *
 * This rule is applied only on columnar project/filter operators (ProjectExecTransformer and
 * FilterExecTransformer) so that row-based SparkPlan (e.g. ProjectExec) keeps using the original
 * `array_contains(map_keys(m), k)` expression, which remains codegen-compatible on the row path.
 */
case class RewriteArrayContainsMapKeys(spark: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    visitPlan(plan)
  }

  private def visitPlan(plan: SparkPlan): SparkPlan = {
    val newPlan = plan match {
      case p: ProjectExecTransformer =>
        val newProjectList = p.projectList.map {
          ne => rewriteExpression(ne).asInstanceOf[NamedExpression]
        }
        p.copy(projectList = newProjectList, child = visitPlan(p.child))

      case f: FilterExecTransformerBase =>
        val newCondition = rewriteExpression(f.cond)
        val newChild = visitPlan(f.child)
        BackendsApiManager.getSparkPlanExecApiInstance
          .genFilterExecTransformer(newCondition, newChild)

      case other =>
        val newChildren = other.children.map(visitPlan)
        other.withNewChildren(newChildren)
    }

    // Preserve tags on SparkPlan nodes (used by fallback and other Gluten utilities).
    newPlan.copyTagsFrom(plan)
    newPlan
  }

  private def rewriteExpression(expr: Expression): Expression = expr match {
    case a: Alias =>
      val newChild = rewriteExpression(a.child)
      if (newChild eq a.child) {
        a
      } else {
        Alias(newChild, a.name)(a.exprId, a.qualifier, a.explicitMetadata)
      }

    // Core rewrite: array_contains(map_keys(m), k) => map_contains_key(m, k)
    case ArrayContains(MapKeys(mapExpr), keyExpr) =>
      val newMapExpr = rewriteExpression(mapExpr)
      val newKeyExpr = rewriteExpression(keyExpr)
      MapContainsKey(newMapExpr, newKeyExpr)

    case other =>
      if (other.children.isEmpty) {
        other
      } else {
        val newChildren = other.children.map(rewriteExpression)
        if (newChildren eq other.children) {
          other
        } else {
          other.withNewChildren(newChildren)
        }
      }
  }
}
