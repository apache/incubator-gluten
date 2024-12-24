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
package org.apache.gluten.extension.injector

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension.GlutenColumnarRule
import org.apache.gluten.extension.columnar.ColumnarRuleApplier
import org.apache.gluten.extension.columnar.ColumnarRuleApplier.ColumnarRuleCall
import org.apache.gluten.extension.columnar.enumerated.{EnumeratedApplier, EnumeratedTransform}
import org.apache.gluten.extension.columnar.enumerated.planner.cost.{LongCoster, LongCostModel}
import org.apache.gluten.extension.columnar.heuristic.{HeuristicApplier, HeuristicTransform}
import org.apache.gluten.ras.CostModel
import org.apache.gluten.ras.rule.RasRule

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.SparkReflectionUtil

import scala.collection.mutable

/** Injector used to inject query planner rules into Gluten. */
class GlutenInjector private[injector] (control: InjectorControl) {
  import GlutenInjector._
  val legacy: LegacyInjector = new LegacyInjector()
  val ras: RasInjector = new RasInjector()

  private[injector] def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(
      control.disabler().wrapColumnarRule(s => new GlutenColumnarRule(s, applier)))
  }

  private def applier(session: SparkSession): ColumnarRuleApplier = {
    val conf = new GlutenConfig(session.sessionState.conf)
    if (conf.enableRas) {
      return ras.createApplier(session)
    }
    legacy.createApplier(session)
  }
}

object GlutenInjector {
  class LegacyInjector {
    private val preTransformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val transformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val postTransformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val fallbackPolicyBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val postBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val finalBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]

    def injectPreTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      preTransformBuilders += builder
    }

    def injectTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      transformBuilders += builder
    }

    def injectPostTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      postTransformBuilders += builder
    }

    def injectFallbackPolicy(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      fallbackPolicyBuilders += builder
    }

    def injectPost(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      postBuilders += builder
    }

    def injectFinal(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      finalBuilders += builder
    }

    private[injector] def createApplier(session: SparkSession): ColumnarRuleApplier = {
      new HeuristicApplier(
        session,
        (preTransformBuilders ++ Seq(
          c => createHeuristicTransform(c)) ++ postTransformBuilders).toSeq,
        fallbackPolicyBuilders.toSeq,
        postBuilders.toSeq,
        finalBuilders.toSeq
      )
    }

    def createHeuristicTransform(call: ColumnarRuleCall): HeuristicTransform = {
      val all = transformBuilders.map(_(call))
      HeuristicTransform.withRules(all.toSeq)
    }
  }

  class RasInjector extends Logging {
    private val preTransformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]
    private val rasRuleBuilders = mutable.Buffer.empty[ColumnarRuleCall => RasRule[SparkPlan]]
    private val costerBuilders = mutable.Buffer.empty[ColumnarRuleCall => LongCoster]
    private val postTransformBuilders = mutable.Buffer.empty[ColumnarRuleCall => Rule[SparkPlan]]

    def injectPreTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      preTransformBuilders += builder
    }

    def injectRasRule(builder: ColumnarRuleCall => RasRule[SparkPlan]): Unit = {
      rasRuleBuilders += builder
    }

    def injectCoster(builder: ColumnarRuleCall => LongCoster): Unit = {
      costerBuilders += builder
    }

    def injectPostTransform(builder: ColumnarRuleCall => Rule[SparkPlan]): Unit = {
      postTransformBuilders += builder
    }

    private[injector] def createApplier(session: SparkSession): ColumnarRuleApplier = {
      new EnumeratedApplier(
        session,
        (preTransformBuilders ++ Seq(
          c => createEnumeratedTransform(c)) ++ postTransformBuilders).toSeq)
    }

    def createEnumeratedTransform(call: ColumnarRuleCall): EnumeratedTransform = {
      // Build RAS rules.
      val rules = rasRuleBuilders.map(_(call))

      // Build the cost model.
      val costModelRegistry = LongCostModel.registry()
      costerBuilders.foreach(cb => costModelRegistry.register(cb(call)))
      val aliasOrClass = call.glutenConf.rasCostModel
      val costModel = findCostModel(costModelRegistry, aliasOrClass)

      // Create transform.
      EnumeratedTransform(costModel, rules.toSeq)
    }

    private def findCostModel(
        registry: LongCostModel.Registry,
        aliasOrClass: String): CostModel[SparkPlan] = {
      if (LongCostModel.Kind.values().contains(aliasOrClass)) {
        val kind = LongCostModel.Kind.values()(aliasOrClass)
        val model = registry.get(kind)
        return model
      }
      val clazz = SparkReflectionUtil.classForName(aliasOrClass)
      logInfo(s"Using user cost model: $aliasOrClass")
      val ctor = clazz.getDeclaredConstructor()
      ctor.setAccessible(true)
      val model: CostModel[SparkPlan] = ctor.newInstance()
      model
    }
  }
}
