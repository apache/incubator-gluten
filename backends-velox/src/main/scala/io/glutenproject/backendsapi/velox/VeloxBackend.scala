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

package io.glutenproject.backendsapi.velox

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.{Backend, IInitializerApi, IIteratorApi, ISparkPlanExecApi, ITransformerApi}

class VeloxBackend extends Backend {
  override def name: String = GlutenConfig.GLUTEN_VELOX_BACKEND
  override def initializerApi(): IInitializerApi = new VeloxInitializerApi
  override def iteratorApi(): IIteratorApi = new VeloxIteratorApi
  override def sparkPlanExecApi(): ISparkPlanExecApi = new VeloxSparkPlanExecApi
  override def transformerApi(): ITransformerApi = new VeloxTransformerApi
}
