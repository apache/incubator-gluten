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

import org.apache.gluten.extension.GlutenPlan
import org.apache.gluten.extension.columnar.{OffloadExchange, OffloadJoin, OffloadOthers, OffloadSingleNode}
import org.apache.gluten.extension.columnar.rewrite.RewriteSingleNode
import org.apache.gluten.extension.columnar.validator.{Validator, Validators}
import org.apache.gluten.planner.GlutenOptimization
import org.apache.gluten.planner.property.Conventions
import org.apache.gluten.ras.property.PropertySet
import org.apache.gluten.ras.rule.{RasRule, Shape, Shapes}
import org.apache.gluten.utils.LogLevelUtil

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

case class EnumeratedTransform(session: SparkSession, outputsColumnar: Boolean)
  extends Rule[SparkPlan]
  with LogLevelUtil {
  import EnumeratedTransform._

  private val validator = Validators
    .builder()
    .fallbackByHint()
    .fallbackIfScanOnly()
    .fallbackComplexExpressions()
    .fallbackByBackendSettings()
    .fallbackByUserOptions()
    .build()

  private val rewrites = RewriteSingleNode.allRules()

  private val rules = List(
    new PushFilterToScan(validator),
    RemoveFilter
  )

  // TODO: Should obey ReplaceSingleNode#applyScanNotTransformable to select
  //  (vanilla) scan with cheaper sub-query plan through cost model.
  private val offloadRules = List(
    new AsRasOffload(OffloadOthers()),
    new AsRasOffload(OffloadExchange()),
    new AsRasOffload(OffloadJoin()),
    RasOffloadAggregate,
    RasOffloadFilter
  ).map(_.withValidator(validator))
    .map(_.withRewrites(rewrites))

  private val optimization = GlutenOptimization(rules ++ offloadRules)

  private val reqConvention = Conventions.ANY
  private val altConventions =
    Seq(Conventions.GLUTEN_COLUMNAR, Conventions.ROW_BASED)

  override def apply(plan: SparkPlan): SparkPlan = {
    val constraintSet = PropertySet(List(reqConvention))
    val altConstraintSets =
      altConventions.map(altConv => PropertySet(List(altConv)))
    val planner = optimization.newPlanner(plan, constraintSet, altConstraintSets)
    val out = planner.plan()
    out
  }
}

object EnumeratedTransform {

  /**
   * Accepts a [[OffloadSingleNode]] rule to convert it into a RAS rule.
   *
   * In the RAS rule, the delegated [[OffloadSingleNode]] will be called to create a offloaded plan
   * node, which will be immediately validated by calling its "doValidate" method to check if it's
   * eligible to run in native. If yes, the offloaded plan node will be returned then added into RAS
   * memo.
   */
  private class AsRasOffload(delegate: OffloadSingleNode) extends RasRule[SparkPlan] {
    override def shift(node: SparkPlan): Iterable[SparkPlan] = {
      val out = delegate.offload(node)
      out match {
        case same if same eq node =>
          List.empty
        case t: GlutenPlan if !t.doValidate().isValid =>
          List.empty
        case other =>
          List(other)
      }
    }

    override def shape(): Shape[SparkPlan] = Shapes.fixedHeight(1)
  }

  implicit private class RasRuleImplicits(rasRule: RasRule[SparkPlan]) {
    def withValidator(v: Validator): RasRule[SparkPlan] = {
      ConditionedRule.wrap(rasRule, v)
    }

    def withRewrites(r: Seq[RewriteSingleNode]): RasRule[SparkPlan] = {
      PreRewrittenRule.wrap(rasRule, r)
    }
  }
}
