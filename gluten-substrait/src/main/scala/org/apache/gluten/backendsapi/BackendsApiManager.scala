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

object BackendsApiManager {
  private lazy val backend: SubstraitBackend = initializeInternal()

  /** Initialize all backends api. */
  private def initializeInternal(): SubstraitBackend = {
    Backend.get().asInstanceOf[SubstraitBackend]
  }

  /**
   * Automatically detect the backend api.
   * @return
   */
  def initialize(): String = {
    getBackendName
  }

  // Note: Do not make direct if-else checks based on output of the method.
  // Any form of backend-specific code should be avoided from appearing in common module
  // (e.g. gluten-substrait, gluten-data)
  def getBackendName: String = {
    backend.name()
  }

  def getListenerApiInstance: ListenerApi = {
    backend.listenerApi()
  }

  def getIteratorApiInstance: IteratorApi = {
    backend.iteratorApi()
  }

  def getSparkPlanExecApiInstance: SparkPlanExecApi = {
    backend.sparkPlanExecApi()
  }

  def getTransformerApiInstance: TransformerApi = {
    backend.transformerApi()
  }

  def getValidatorApiInstance: ValidatorApi = {
    backend.validatorApi()
  }

  def getMetricsApiInstance: MetricsApi = {
    backend.metricsApi()
  }

  def getRuleApiInstance: RuleApi = {
    backend.ruleApi()
  }

  def getSettings: BackendSettingsApi = {
    backend.settings
  }
}
