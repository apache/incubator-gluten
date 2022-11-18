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

import org.apache.spark.SparkConf
import io.glutenproject.vectorized.JniLibLoader
import io.glutenproject.vectorized.JniWorkspace
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.IInitializerApi
import org.apache.commons.lang3.StringUtils

class GazelleInitializerApi extends IInitializerApi {
  override def initialize(conf: SparkConf): Unit = {
    val workspace = JniWorkspace.getDefault
    val loader = workspace.libLoader
    loader.newTransaction()
      .loadAndCreateLink("libarrow.so.800.0.0", "libarrow.so.800", false)
      .loadAndCreateLink("libparquet.so.800.0.0", "libparquet.so.800", false)
      .loadAndCreateLink("libarrow_dataset.so.800.0.0", "libarrow_dataset.so.800", false)
      .loadAndCreateLink("libarrow_substrait.so.800.0.0", "libarrow_substrait.so.800", false)
      .commit()
    val libPath = conf.get(GlutenConfig.GLUTEN_LIB_PATH, StringUtils.EMPTY)
    if (StringUtils.isNotBlank(libPath)) { // Path based load. Ignore all other loadees.
      JniLibLoader.loadFromPath(libPath, true)
      return
    }
    val baseLibName = conf.get(GlutenConfig.GLUTEN_LIB_NAME, "spark_columnar_jni")
    loader.mapAndLoad(baseLibName, true)
    loader.mapAndLoad(GlutenConfig.GLUTEN_GAZELLE_BACKEND, false)
  }
}
