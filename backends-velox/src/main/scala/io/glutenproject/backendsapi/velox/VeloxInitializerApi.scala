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
import io.glutenproject.backendsapi.IInitializerApi
import io.glutenproject.vectorized.{JniLibLoader, JniWorkspace, VeloxNativeExpressionEvaluator}
import io.glutenproject.GlutenPlugin.buildNativeConfNode
import org.apache.commons.lang3.StringUtils

import org.apache.spark.SparkConf

class VeloxInitializerApi extends IInitializerApi {

  override def initialize(conf: SparkConf): Unit = {
    val workspace = JniWorkspace.getDefault
    val loader = workspace.libLoader
    loader.newTransaction()
      .loadAndCreateLink("libarrow.so.1000.0.0", "libarrow.so.1000", false)
      .commit()
    val libPath = conf.get(GlutenConfig.GLUTEN_LIB_PATH, StringUtils.EMPTY)
    if (StringUtils.isNotBlank(libPath)) { // Path based load. Ignore all other loadees.
      JniLibLoader.loadFromPath(libPath, true)
      return
    }
    val baseLibName = conf.get(GlutenConfig.GLUTEN_LIB_NAME, "spark_columnar_jni")
    loader.mapAndLoad(baseLibName, true)
    loader.mapAndLoad(GlutenConfig.GLUTEN_VELOX_BACKEND, true)
    val initKernel = new VeloxNativeExpressionEvaluator()
    initKernel.initNative(buildNativeConfNode(conf).toProtobuf.toByteArray)
  }
}
