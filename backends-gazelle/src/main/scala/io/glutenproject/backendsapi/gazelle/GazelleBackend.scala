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
package io.glutenproject.backendsapi.gazelle

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi._

import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat
import io.glutenproject.substrait.rel.LocalFilesNode.ReadFileFormat.ParquetReadFormat

// FIXME The backend reuses some of Velox BE's code. Cleanup is needed
//  to avoid this.
class GazelleBackend extends Backend {
  override def name(): String = GlutenConfig.GLUTEN_GAZELLE_BACKEND
  override def initializerApi(): IInitializerApi = new GazelleInitializerApi
  override def iteratorApi(): IIteratorApi = new GazelleIteratorApi
  override def sparkPlanExecApi(): ISparkPlanExecApi = new GazelleSparkPlanExecApi
  override def transformerApi(): ITransformerApi = new GazelleTransformerApi
  override def validatorApi(): IValidatorApi = new GazelleValidatorApi
  override def settings(): BackendSettings = GazelleBackendSettings
}

object GazelleBackendSettings extends BackendSettings {
  override def supportFileFormatRead: ReadFileFormat => Boolean = {
    case ParquetReadFormat => true
    case _ => false
  }

  override def disableVanillaColumnarReaders(): Boolean = true
  override def fallbackOnEmptySchema(): Boolean = true
  override def supportColumnarShuffleExec(): Boolean = false
  override def avoidOverwritingFilterTransformer(): Boolean = true
  override def fallbackFilterWithoutConjunctiveScan(): Boolean = true

  /**
   * Get the config prefix for each backend
   */
  override def getBackendConfigPrefix(): String =
    GlutenConfig.GLUTEN_CONFIG_PREFIX + GlutenConfig.GLUTEN_GAZELLE_BACKEND
}
