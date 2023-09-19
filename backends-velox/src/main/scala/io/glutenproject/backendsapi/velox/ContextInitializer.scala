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
import io.glutenproject.execution.datasource.GlutenOrcWriterInjects
import io.glutenproject.execution.datasource.GlutenParquetWriterInjects
import io.glutenproject.execution.datasource.GlutenRowSplitter
import io.glutenproject.expression.UDFMappings
import io.glutenproject.utils._
import io.glutenproject.vectorized.{JniLibLoader, JniWorkspace}

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.velox.VeloxOrcWriterInjects
import org.apache.spark.sql.execution.datasources.velox.VeloxParquetWriterInjects
import org.apache.spark.sql.execution.datasources.velox.VeloxRowSplitter
import org.apache.spark.util.TaskResource

import org.apache.commons.lang3.StringUtils

import scala.sys.process._

class ContextInitializer extends ContextApi {
  private def loadLibFromJar(load: JniLibLoader): Unit = {
    val system = "cat /etc/os-release".!!
    val systemNamePattern = "^NAME=\"?(.*)\"?".r
    val systemVersionPattern = "^VERSION=\"?(.*)\"?".r
    val systemInfoLines = system.stripMargin.split("\n")
    val systemNamePattern(systemName) =
      systemInfoLines.find(_.startsWith("NAME=")).getOrElse("")
    val systemVersionPattern(systemVersion) =
      systemInfoLines.find(_.startsWith("VERSION=")).getOrElse("")
    if (systemName.isEmpty || systemVersion.isEmpty) {
      throw new GlutenException("Failed to get OS name and version info.")
    }
    val loader = if (systemName.contains("Ubuntu") && systemVersion.startsWith("20.04")) {
      new SharedLibraryLoaderUbuntu2004
    } else if (systemName.contains("Ubuntu") && systemVersion.startsWith("22.04")) {
      new SharedLibraryLoaderUbuntu2204
    } else if (systemName.contains("CentOS") && systemVersion.startsWith("8")) {
      new SharedLibraryLoaderCentos8
    } else if (systemName.contains("CentOS") && systemVersion.startsWith("7")) {
      new SharedLibraryLoaderCentos7
    } else if (systemName.contains("Alibaba Cloud Linux") && systemVersion.startsWith("3")) {
      new SharedLibraryLoaderCentos8
    } else if (systemName.contains("Alibaba Cloud Linux") && systemVersion.startsWith("2")) {
      new SharedLibraryLoaderCentos7
    } else if (systemName.contains("Anolis") && systemVersion.startsWith("8")) {
      new SharedLibraryLoaderCentos8
    } else if (systemName.contains("Anolis") && systemVersion.startsWith("7")) {
      new SharedLibraryLoaderCentos7
    } else if (system.contains("tencentos") && system.contains("3.2")) {
      new SharedLibraryLoaderCentos8
    } else {
      throw new GlutenException(
        "Found unsupported OS! Currently, Gluten's Velox backend" +
          " only supports Ubuntu 20.04/22.04, CentOS 7/8, " +
          "Alibaba Cloud Linux 2/3 & Anolis 7/8, tencentos 3.2.")
    }
    loader.loadLib(load)
  }

  private def loadLibWithLinux(conf: SparkConf, loader: JniLibLoader): Unit = {
    if (
      conf.getBoolean(
        GlutenConfig.GLUTEN_LOAD_LIB_FROM_JAR,
        GlutenConfig.GLUTEN_LOAD_LIB_FROM_JAR_DEFAULT)
    ) {
      loadLibFromJar(loader)
    }
    loader
      .newTransaction()
      .loadAndCreateLink("libarrow.so.1300.0.0", "libarrow.so.1300", false)
      .loadAndCreateLink("libparquet.so.1300.0.0", "libparquet.so.1300", false)
      .commit()
  }

  private def loadLibWithMacOS(loader: JniLibLoader): Unit = {
    loader
      .newTransaction()
      .loadAndCreateLink("libarrow.1200.0.0.dylib", "libarrow.1200.dylib", false)
      .loadAndCreateLink("libparquet.1200.0.0.dylib", "libparquet.1200.dylib", false)
      .commit()
  }

  override def taskResourceFactories(): Seq[() => TaskResource] = Seq.empty

  override def initialize(conf: SparkConf): Unit = {
    val workspace = JniWorkspace.getDefault
    val loader = workspace.libLoader

    val osName = System.getProperty("os.name")
    if (osName.startsWith("Mac OS X") || osName.startsWith("macOS")) {
      loadLibWithMacOS(loader)
    } else {
      loadLibWithLinux(conf, loader)
    }

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
    GlutenOrcWriterInjects.setInstance(new VeloxOrcWriterInjects())
    GlutenRowSplitter.setInstance(new VeloxRowSplitter())
  }

  override def shutdown(): Unit = {
    // TODO shutdown implementation in velox to release resources
  }
}
