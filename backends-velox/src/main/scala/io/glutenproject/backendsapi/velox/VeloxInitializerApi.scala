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
  def loadLibFromJarUbuntu2204(load: JniLibLoader) : Unit = {

  }
  def loadLibFromJarUbuntu2004(load: JniLibLoader) : Unit = {
    loader.newTransaction()
      .loadAndCreateLink("libroken.so.18", "libroken.so", false)
      .loadAndCreateLink("libasn1.so.8", "libasn1.so", false)
      .loadAndCreateLink("libboost_context.so.1.71.0", "libboost_context.so", false)
      .loadAndCreateLink("libboost_regex.so.1.71.0", "libboost_regex.so", false)
      .loadAndCreateLink("libbrotlicommon.so.1", "libbrotlicommon.so", false)
      .loadAndCreateLink("libbrotlidec.so.1", "libbrotlidec.so", false)
      .loadAndCreateLink("libpsl.so.5", "libpsl.so", false)
      .loadAndCreateLink("libcrypto.so.1.1", "libcrypto.so", false)
      .loadAndCreateLink("libnghttp2.so.14", "libnghttp2.so", false)
      .loadAndCreateLink("libnettle.so.7", "libnettle.so", false)
      .loadAndCreateLink("libhogweed.so.5", "libhogweed.so", false)
      .loadAndCreateLink("librtmp.so.1", "librtmp.so", false)
      .loadAndCreateLink("libssh.so.4", "libssh.so", false)
      .loadAndCreateLink("libssl.so.1.1", "libssl.so", false)
      .loadAndCreateLink("liblber-2.4.so.2", "liblber-2.4.so", false)
      .loadAndCreateLink("libsasl2.so.2", "libsasl2.so", false)
      .loadAndCreateLink("libwind.so.0", "libwind.so", false)
      .loadAndCreateLink("libheimbase.so.1", "libheimbase.so", false)
      .loadAndCreateLink("libhcrypto.so.4", "libhcrypto.so", false)
      .loadAndCreateLink("libhx509.so.5", "libhx509.so", false)
      .loadAndCreateLink("libkrb5.so.26", "libkrb5.so", false)
      .loadAndCreateLink("libheimntlm.so.0", "libheimntlm.so", false)
      .loadAndCreateLink("libgssapi.so.3", "libgssapi.so", false)
      .loadAndCreateLink("libldap_r-2.4.so.2", "libldap_r-2.4.so", false)
      .loadAndCreateLink("libcurl.so.4", "libcurl.so", false)
      .loadAndCreateLink("libdouble-conversion.so.3", "libdouble-conversion.so", false)
      .loadAndCreateLink("libevent-2.1.so.7", "libevent-2.1.so", false)
      .loadAndCreateLink("libgflags.so.2.2", "libgflags.so", false)
      .loadAndCreateLink("libunwind.so.8", "libunwind.so", false)
      .loadAndCreateLink("libglog.so.0", "libglog.so", false)
      .loadAndCreateLink("libidn.so.11", "libidn.so", false)
      .loadAndCreateLink("libntlm.so.0", "libntlm.so", false)
      .loadAndCreateLink("libgsasl.so.7", "libgsasl.so", false)
      .loadAndCreateLink("libprotobuf.so.32", "libprotobuf.so", false)
      .loadAndCreateLink("libicudata.so.66", "libicudata.so", false)
      .loadAndCreateLink("libicuuc.so.66", "libicuuc.so", false)
      .loadAndCreateLink("libxml2.so.2", "libxml2.so", false)
      .loadAndCreateLink("libhdfs3.so.1", "libhdfs3.so", false)
      .loadAndCreateLink("libre2.so.5", "libre2.so", false)
      .loadAndCreateLink("libsnappy.so.1", "libsnappy.so", false)
      .commit()
  }

  def loadLibFromJar(load: JniLibLoader): Unit = {
      val system = "cat /etc/os-release".!!
      if (system.contains("Ubuntu") && system.contains("20.04")) {
        loadLibFromJarUbuntu2004(load)
      }
      if (system.contains("Ubuntu") && system.contains("22.04")) {
        loadLibFromJarUbuntu2204(load)
      }
  }

  override def initialize(conf: SparkConf): Unit = {
    val workspace = JniWorkspace.getDefault
    val loader = workspace.libLoader
    if (conf.getBoolean(GlutenConfig.GLUTEN_LOAD_LIB_FROM_JARS, false)) {
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
