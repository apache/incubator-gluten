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
import io.glutenproject.backendsapi.InitializerApi
import io.glutenproject.vectorized.{GlutenNativeExpressionEvaluator, JniLibLoader, JniWorkspace}
import org.apache.commons.lang3.StringUtils
import scala.collection.JavaConverters._
import scala.sys.process._

import org.apache.spark.SparkConf

class VeloxInitializerApi extends InitializerApi {
  def loadLibFromJar(load: JniLibLoader): Unit = {
      val system = "cat /etc/os-release".!!
      val veloxDllLoader = if (system.contains("Ubuntu") && system.contains("20.04")) {
        new VeloxDllLoaderUbuntu2004
      } else if (system.contains("Ubuntu") && system.contains("22.04")) {
        new VeloxDllLoaderUbuntu2204
      }
      veloxDllLoader.loadLib(load)
  }

  override def initialize(conf: SparkConf): Unit = {
    val workspace = JniWorkspace.getDefault
    val loader = workspace.libLoader
    if (GlutenConfig.getConf.loadLibFromJars) {
      loadLibFromJar(loader)
    }
    loader.newTransaction()
      .loadAndCreateLink("libarrow.so.1100.0.0", "libarrow.so.1100", false)
      .loadAndCreateLink("libparquet.so.1100.0.0", "libparquet.so.1100", false)
      .commit()
    if (conf.getBoolean(GlutenConfig.GLUTEN_ENABLE_QAT, false)) {
      loader.newTransaction()
        .loadAndCreateLink("libqatzip.so.3.0.1", "libqatzip.so.3", false)
        .commit()
    }
    val libPath = conf.get(GlutenConfig.GLUTEN_LIB_PATH, StringUtils.EMPTY)
    if (StringUtils.isNotBlank(libPath)) { // Path based load. Ignore all other loadees.
      JniLibLoader.loadFromPath(libPath, true)
      return
    }
    val baseLibName = conf.get(GlutenConfig.GLUTEN_LIB_NAME, "gluten")
    loader.mapAndLoad(baseLibName, true)
    loader.mapAndLoad(GlutenConfig.GLUTEN_VELOX_BACKEND, true)
    val initKernel = new GlutenNativeExpressionEvaluator()
    conf.setAll(GlutenConfig.createGeneratedConf(conf).asScala)
    initKernel.initNative(conf)
  }
}
