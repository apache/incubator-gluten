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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.extension.GlutenColumnarRule
import org.apache.gluten.extension.columnar.ColumnarRuleApplier
import org.apache.gluten.extension.columnar.ColumnarRuleApplier.{ColumnarRuleBuilder, SkipCondition}
import org.apache.gluten.extension.columnar.enumerated.EnumeratedApplier
import org.apache.gluten.extension.columnar.heuristic.HeuristicApplier

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}

import scala.collection.mutable

/** Injector used to inject query planner rules into Gluten. */
class GlutenInjector private[injector] {
  import GlutenInjector._
  private val skipConditions: mutable.ListBuffer[SkipCondition] = mutable.ListBuffer()
  val legacy: LegacyInjector = new LegacyInjector()
  val ras: RasInjector = new RasInjector()

  private[injector] def inject(extensions: SparkSessionExtensions): Unit = {
    extensions.injectColumnar(session => new GlutenColumnarRule(session, applier))
  }

  private def applier(session: SparkSession): ColumnarRuleApplier = {
    val conf = new GlutenConfig(session.sessionState.conf)
    if (conf.enableRas) {
      return ras.createApplier(session, skipConditions.toSeq)
    }
    legacy.createApplier(session, skipConditions.toSeq)
  }

  def skipOn(skipCondition: SkipCondition): Unit = {
    skipConditions += skipCondition
  }
}

object GlutenInjector {
  class LegacyInjector {
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

    private[injector] def createApplier(
        session: SparkSession,
        skipConditions: Seq[SkipCondition]): ColumnarRuleApplier = {
      new HeuristicApplier(
        session,
        skipConditions,
        transformBuilders.toSeq,
        fallbackPolicyBuilders.toSeq,
        postBuilders.toSeq,
        finalBuilders.toSeq)
    }
  }

  class RasInjector {
    private val ruleBuilders = mutable.Buffer.empty[ColumnarRuleBuilder]

    def inject(builder: ColumnarRuleBuilder): Unit = {
      ruleBuilders += builder
    }

    private[injector] def createApplier(
        session: SparkSession,
        skipConditions: Seq[SkipCondition]): ColumnarRuleApplier = {
      new EnumeratedApplier(session, skipConditions, ruleBuilders.toSeq)
    }
  }
}
