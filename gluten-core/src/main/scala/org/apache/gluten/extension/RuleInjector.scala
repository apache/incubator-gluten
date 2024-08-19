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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.extension.columnar.ColumnarRuleApplier.ColumnarRuleBuilder
import org.apache.gluten.extension.columnar.enumerated.EnumeratedApplier
import org.apache.gluten.extension.columnar.heuristic.HeuristicApplier

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.mutable

class RuleInjector {
  import RuleInjector._

  val spark: SparkInjector = SparkInjector()
  val gluten: GlutenInjector = GlutenInjector()
  val ras: RasInjector = RasInjector()

  private[extension] def inject(extensions: SparkSessionExtensions): Unit = {
    spark.inject(extensions)
    if (GlutenConfig.getConf.enableRas) {
      ras.inject(extensions)
    } else {
      gluten.inject(extensions)
    }
  }
}

object RuleInjector {
  class SparkInjector private {
    private type RuleBuilder = SparkSession => Rule[LogicalPlan]
    private type StrategyBuilder = SparkSession => Strategy
    private type ParserBuilder = (SparkSession, ParserInterface) => ParserInterface
    private type FunctionDescription = (FunctionIdentifier, ExpressionInfo, FunctionBuilder)
    private type QueryStagePrepRuleBuilder = SparkSession => Rule[SparkPlan]

    private val queryStagePrepRuleBuilders = mutable.Buffer.empty[QueryStagePrepRuleBuilder]
    private val parserBuilders = mutable.Buffer.empty[ParserBuilder]
    private val resolutionRuleBuilders = mutable.Buffer.empty[RuleBuilder]
    private val optimizerRules = mutable.Buffer.empty[RuleBuilder]
    private val plannerStrategyBuilders = mutable.Buffer.empty[StrategyBuilder]
    private val injectedFunctions = mutable.Buffer.empty[FunctionDescription]
    private val postHocResolutionRuleBuilders = mutable.Buffer.empty[RuleBuilder]

    def injectQueryStagePrepRule(builder: QueryStagePrepRuleBuilder): Unit = {
      queryStagePrepRuleBuilders += builder
    }

    def injectParser(builder: ParserBuilder): Unit = {
      parserBuilders += builder
    }

    def injectResolutionRule(builder: RuleBuilder): Unit = {
      resolutionRuleBuilders += builder
    }

    def injectOptimizerRule(builder: RuleBuilder): Unit = {
      optimizerRules += builder
    }

    def injectPlannerStrategy(builder: StrategyBuilder): Unit = {
      plannerStrategyBuilders += builder
    }

    def injectFunction(functionDescription: FunctionDescription): Unit = {
      injectedFunctions += functionDescription
    }

    def injectPostHocResolutionRule(builder: RuleBuilder): Unit = {
      postHocResolutionRuleBuilders += builder
    }

    private[extension] def inject(extensions: SparkSessionExtensions): Unit = {
      queryStagePrepRuleBuilders.foreach(extensions.injectQueryStagePrepRule)
      parserBuilders.foreach(extensions.injectParser)
      resolutionRuleBuilders.foreach(extensions.injectResolutionRule)
      optimizerRules.foreach(extensions.injectOptimizerRule)
      plannerStrategyBuilders.foreach(extensions.injectPlannerStrategy)
      injectedFunctions.foreach(extensions.injectFunction)
      postHocResolutionRuleBuilders.foreach(extensions.injectPostHocResolutionRule)
    }
  }

  class GlutenInjector private {
    private val transformBuilders = mutable.Buffer.empty[ColumnarRuleBuilder]
    private val fallbackPolicyBuilders = mutable.Buffer.empty[ColumnarRuleBuilder]
    private val postBuilders = mutable.Buffer.empty[ColumnarRuleBuilder]
    private val finalBuilders = mutable.Buffer.empty[ColumnarRuleBuilder]

    def injectTransform(builder: ColumnarRuleBuilder): Unit = {
      transformBuilders += builder
    }

    def injectFallbackPolicy(builder: ColumnarRuleBuilder): Unit = {
      fallbackPolicyBuilders += builder
    }

    def injectPost(builder: ColumnarRuleBuilder): Unit = {
      postBuilders += builder
    }

    def injectFinal(builder: ColumnarRuleBuilder): Unit = {
      finalBuilders += builder
    }

    private[extension] def inject(extensions: SparkSessionExtensions): Unit = {
      val applierBuilder = (session: SparkSession) =>
        new HeuristicApplier(
          session,
          transformBuilders,
          fallbackPolicyBuilders,
          postBuilders,
          finalBuilders)
      val ruleBuilder = (session: SparkSession) =>
        new ColumnarOverrideRules(session, applierBuilder(session))
      extensions.injectColumnar(session => ruleBuilder(session))
    }
  }

  class RasInjector private {
    private val ruleBuilders = mutable.Buffer.empty[ColumnarRuleBuilder]

    def inject(builder: ColumnarRuleBuilder): Unit = {
      ruleBuilders += builder
    }

    private[extension] def inject(extensions: SparkSessionExtensions): Unit = {
      val applierBuilder = (session: SparkSession) => new EnumeratedApplier(session, ruleBuilders)
      val ruleBuilder = (session: SparkSession) =>
        new ColumnarOverrideRules(session, applierBuilder(session))
      extensions.injectColumnar(session => ruleBuilder(session))
    }

  }

  private object SparkInjector {
    def apply(): SparkInjector = {
      new SparkInjector()
    }
  }

  private object GlutenInjector {
    def apply(): GlutenInjector = {
      new GlutenInjector()
    }
  }
  private object RasInjector {
    def apply(): RasInjector = {
      new RasInjector()
    }
  }
}
