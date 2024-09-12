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
package org.apache.gluten.backendsapi

import org.apache.gluten.backend.Backend
import org.apache.gluten.extension.injector.RuleInjector

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.PluginContext

trait SubstraitBackend extends Backend {
  final override def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = {
    listenerApi().onDriverStart(sc, pc)
  }
  final override def onDriverShutdown(): Unit = {
    listenerApi().onDriverShutdown()
  }
  final override def onExecutorStart(pc: PluginContext): Unit = {
    listenerApi().onExecutorStart(pc)
  }
  final override def onExecutorShutdown(): Unit = {
    listenerApi().onExecutorShutdown()
  }
  final override def injectRules(injector: RuleInjector): Unit = {
    ruleApi().injectRules(injector)
  }
  def iteratorApi(): IteratorApi
  def sparkPlanExecApi(): SparkPlanExecApi
  def transformerApi(): TransformerApi
  def validatorApi(): ValidatorApi
  def metricsApi(): MetricsApi
  def listenerApi(): ListenerApi
  def ruleApi(): RuleApi
  def settings(): BackendSettingsApi
}
