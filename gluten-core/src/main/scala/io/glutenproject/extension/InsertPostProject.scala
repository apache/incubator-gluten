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
package io.glutenproject.extension

import io.glutenproject.execution.{HashAggregateExecBaseTransformer, ProjectExecTransformer}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

object InsertPostProject extends Rule[SparkPlan] with AliasHelper {

  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case agg: HashAggregateExecBaseTransformer if agg.needsPostProjection =>
      val aggResultAttributeSet = ExpressionSet(agg.allAggregateResultAttributes)
      // Need to remove columns that are not part of the aggregation result attributes.
      // Although this won't affect the result, it may cause some issues with subquery
      // reuse. For example, if a subquery is referenced by both resultExpressions and
      // post-project, it may result in some unit tests failing that verify the number
      // of GlutenPlan.
      val rewrittenResultExpressions =
        agg.resultExpressions.filter(_.find(aggResultAttributeSet.contains).isDefined)
      val projectList = agg.resultExpressions.map {
        case ne: NamedExpression => ne
        case other => Alias(other, other.toString())()
      }
      ProjectExecTransformer(
        projectList,
        agg.copySelf(resultExpressions = rewrittenResultExpressions),
        isPostProject = true)
  }

  /**
   * This function is used to generate the transformed plan during validation, and it can return the
   * inserted post-project, as well as the operator transformer. There are two different scenarios
   * in total.
   *   - post-projection -> transformer
   *   - transformer
   */
  def getTransformedPlan(transformer: SparkPlan): Seq[SparkPlan] = {
    val transformedPlan = apply(transformer)
    transformedPlan match {
      case p: ProjectExecTransformer if p.isPostProject =>
        val transformedOriginPlan = p.child
        transformedPlan :: transformedOriginPlan :: Nil
      case _ =>
        transformedPlan :: Nil
    }
  }
}
