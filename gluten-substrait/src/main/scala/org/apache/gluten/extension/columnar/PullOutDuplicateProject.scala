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
import org.apache.gluten.execution.{BroadcastHashJoinExecTransformerBase, LimitExecTransformer, ProjectExecTransformer}

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeSet, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._

import scala.collection.mutable.ArrayBuffer

/**
 * Velox does not allow duplicate projections in hash probe, this rule pull out duplicate
 * projections to a new project outside the join.
 */
object PullOutDuplicateProject extends Rule[SparkPlan] with PredicateHelper {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case bhj: BroadcastHashJoinExecTransformerBase =>
      val pullOutAliases = new ArrayBuffer[Alias]()
      val streamedPlan = rewriteStreamedPlan(bhj.streamedPlan, bhj.references, pullOutAliases)
      if (pullOutAliases.isEmpty) {
        bhj
      } else {
        val aliasMap = AttributeMap(pullOutAliases.map(a => a.toAttribute -> a))
        val newProjectList = bhj.output.map(attr => aliasMap.getOrElse(attr, attr))
        val (newLeft, newRight) = bhj.joinBuildSide match {
          case BuildLeft => (bhj.left, streamedPlan)
          case BuildRight => (streamedPlan, bhj.right)
        }

        val newBhj =
          BackendsApiManager.getSparkPlanExecApiInstance.genBroadcastHashJoinExecTransformer(
            bhj.leftKeys,
            bhj.rightKeys,
            bhj.hashJoinType,
            bhj.joinBuildSide,
            bhj.condition,
            newLeft,
            newRight,
            bhj.genJoinParametersInternal()._2 == 1)
        ProjectExecTransformer(newProjectList, newBhj)
      }
  }

  /**
   * If there are duplicate projections in the streamed plan of the join, only the original
   * attribute is kept in the project.
   */
  private def rewriteStreamedPlan(
      plan: SparkPlan,
      references: AttributeSet,
      pullOutAliases: ArrayBuffer[Alias]): SparkPlan = plan match {
    case l @ LimitExecTransformer(child, _, _) =>
      val newChild = rewriteStreamedPlan(child, references, pullOutAliases)
      if (pullOutAliases.isEmpty) {
        l
      } else {
        l.copy(child = newChild)
      }
    case p @ ProjectExecTransformer(projectList, _) =>
      val duplicates = AttributeSet(
        projectList
          .collect {
            case attr: Attribute if !references.contains(attr) => attr
            case a @ Alias(attr: Attribute, _)
                if !references.contains(a) && !references.contains(attr) =>
              attr
          }
          .groupBy(_.exprId)
          .filter(_._2.size > 1)
          .map(_._2.head))
      if (duplicates.nonEmpty) {
        val newProjectList = projectList.filter {
          case a @ Alias(attr: Attribute, _) if duplicates.contains(attr) =>
            pullOutAliases.append(a)
            false
          case _ => true
        } ++ duplicates.filter(!p.outputSet.contains(_)).toSeq
        p.copy(projectList = newProjectList)
      } else {
        p
      }
    case _ => plan
  }
}
