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
import io.glutenproject.backendsapi.ContextApi
import io.glutenproject.exception.GlutenException
import io.glutenproject.execution.datasource.GlutenParquetWriterInjects
import io.glutenproject.expression.UDFMappings
import io.glutenproject.init.JniTaskContext
import io.glutenproject.utils._
import io.glutenproject.vectorized.{JniLibLoader, JniWorkspace}

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.velox.VeloxParquetWriterInjects
import org.apache.spark.util.TaskResource

import org.apache.commons.lang3.StringUtils

import scala.sys.process._

class ContextInitializer extends ContextApi {
  private def loadLibFromJar(load: JniLibLoader): Unit = {
    val system = "cat /etc/os-release".!!
    val loader = if (system.contains("Ubuntu") && system.contains("20.04")) {
      new SharedLibraryLoaderUbuntu2004
    } else if (system.contains("Ubuntu") && system.contains("22.04")) {
      new SharedLibraryLoaderUbuntu2204
    } else if (system.contains("CentOS") && system.contains("8")) {
      new SharedLibraryLoaderCentos8
    } else if (system.contains("CentOS") && system.contains("7")) {
      new SharedLibraryLoaderCentos7
    } else if (system.contains("alinux") && system.contains("3")) {
      new SharedLibraryLoaderCentos8
    } else if (system.contains("Anolis") && system.contains("8")) {
      new SharedLibraryLoaderCentos8
    } else if (system.contains("Anolis") && system.contains("7")) {
      new SharedLibraryLoaderCentos7
    } else {
      throw new GlutenException(
        "Found unsupported OS! Currently, Gluten's Velox backend" +
          " only supports Ubuntu 20.04/22.04, CentOS 7/8, alinux 3 & Anolis 7/8.")
    }
    loader.loadLib(load)
  }

  override def taskResourceFactories(): Seq[() => TaskResource] = {
    Seq(() => new JniTaskContext())
  }

  override def initialize(conf: SparkConf): Unit = {
    val workspace = JniWorkspace.getDefault
    val loader = workspace.libLoader
    if (
      conf.getBoolean(
        GlutenConfig.GLUTEN_LOAD_LIB_FROM_JAR,
        GlutenConfig.GLUTEN_LOAD_LIB_FROM_JAR_DEFAULT)
    ) {
      loadLibFromJar(loader)
    }
    loader
      .newTransaction()
      .loadAndCreateLink("libarrow.so.800.0.0", "libarrow.so.800", false)
      .loadAndCreateLink("libparquet.so.800.0.0", "libparquet.so.800", false)
      .commit()
    // Set the system properties.
    // Use appending policy for children with the same name in a arrow struct vector.
    System.setProperty("arrow.struct.conflict.policy", "CONFLICT_APPEND")

    // Load supported hive/python/scala udfs
    UDFMappings.loadFromSparkConf(conf)

    val libPath = conf.get(GlutenConfig.GLUTEN_LIB_PATH, StringUtils.EMPTY)
    if (StringUtils.isNotBlank(libPath)) { // Path based load. Ignore all other loadees.
      JniLibLoader.loadFromPath(libPath, true)
      return
    }
    val baseLibName = conf.get(GlutenConfig.GLUTEN_LIB_NAME, "gluten")
    loader.mapAndLoad(baseLibName, true)
    loader.mapAndLoad(GlutenConfig.GLUTEN_VELOX_BACKEND, true)

    // inject backend-specific implementations to override spark classes
    GlutenParquetWriterInjects.setInstance(new VeloxParquetWriterInjects())
  }

  override def shutdown(): Unit = {
    // TODO shutdown implementation in velox to release resources
  }
}
