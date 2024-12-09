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
package org.apache.gluten.extension.columnar.enumerated.planner

import org.apache.gluten.extension.columnar.enumerated.planner.metadata.GlutenMetadataModel
import org.apache.gluten.extension.columnar.enumerated.planner.plan.GlutenPlanModel
import org.apache.gluten.extension.columnar.enumerated.planner.property.GlutenPropertyModel
import org.apache.gluten.ras.{CostModel, Optimization, RasExplain}
import org.apache.gluten.ras.rule.RasRule

import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable

object GlutenOptimization {
  def builder(): Builder = new BuilderImpl

  private object GlutenExplain extends RasExplain[SparkPlan] {
    override def describeNode(node: SparkPlan): String = node.nodeName
  }

  trait Builder {
    def addRules(rules: Seq[RasRule[SparkPlan]]): Builder
    def costModel(costModel: CostModel[SparkPlan]): Builder
    def create(): Optimization[SparkPlan]
  }

  private class BuilderImpl extends Builder {
    private val rules: mutable.ListBuffer[RasRule[SparkPlan]] = mutable.ListBuffer()
    private var costModel: Option[CostModel[SparkPlan]] = None

    override def addRules(rules: Seq[RasRule[SparkPlan]]): Builder = {
      this.rules ++= rules
      this
    }

    override def costModel(costModel: CostModel[SparkPlan]): Builder = {
      this.costModel = Some(costModel)
      this
    }

    override def create(): Optimization[SparkPlan] = {
      assert(costModel.isDefined, "Cost model is required to initialize GlutenOptimization")
      Optimization[SparkPlan](
        GlutenPlanModel(),
        costModel.get,
        GlutenMetadataModel(),
        GlutenPropertyModel(),
        GlutenExplain,
        RasRule.Factory.reuse(rules.toSeq))
    }
  }
}
