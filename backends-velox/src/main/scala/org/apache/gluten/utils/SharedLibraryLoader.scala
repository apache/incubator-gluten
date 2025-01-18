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
package org.apache.gluten.utils

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.jni.JniLibLoader

import org.apache.spark.SparkConf

import scala.sys.process._

trait SharedLibraryLoader {
  def loadLib(loader: JniLibLoader): Unit
}

object SharedLibraryLoader {
  def load(conf: SparkConf, jni: JniLibLoader): Unit = {
    val shouldLoad = conf.getBoolean(
      GlutenConfig.GLUTEN_LOAD_LIB_FROM_JAR.key,
      GlutenConfig.GLUTEN_LOAD_LIB_FROM_JAR.defaultValue.get)
    if (!shouldLoad) {
      return
    }
    val osName = System.getProperty("os.name")
    if (osName.startsWith("Mac OS X") || osName.startsWith("macOS")) {
      loadLibWithMacOS(jni)
    } else {
      loadLibWithLinux(conf, jni)
    }
  }

  private def loadLibWithLinux(conf: SparkConf, jni: JniLibLoader): Unit = {
    val loader = find(conf)
    loader.loadLib(jni)
  }

  private def loadLibWithMacOS(jni: JniLibLoader): Unit = {
    // Placeholder for loading shared libs on MacOS if user needs.
  }

  private def find(conf: SparkConf): SharedLibraryLoader = {
    val systemName = conf.getOption(GlutenConfig.GLUTEN_LOAD_LIB_OS.key)
    val loader = if (systemName.isDefined) {
      val systemVersion = conf.getOption(GlutenConfig.GLUTEN_LOAD_LIB_OS_VERSION.key)
      if (systemVersion.isEmpty) {
        throw new GlutenException(
          s"${GlutenConfig.GLUTEN_LOAD_LIB_OS_VERSION.key} must be specified when specifies the " +
            s"${GlutenConfig.GLUTEN_LOAD_LIB_OS.key}")
      }
      getForOS(systemName.get, systemVersion.get, "")
    } else {
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
      getForOS(systemName, systemVersion, system)
    }
    loader
  }

  private def getForOS(
      systemName: String,
      systemVersion: String,
      system: String): SharedLibraryLoader = {
    if (systemName.contains("Ubuntu") && systemVersion.startsWith("20.04")) {
      new SharedLibraryLoaderUbuntu2004
    } else if (systemName.contains("Ubuntu") && systemVersion.startsWith("22.04")) {
      new SharedLibraryLoaderUbuntu2204
    } else if (systemName.contains("CentOS") && systemVersion.startsWith("9")) {
      new SharedLibraryLoaderCentos9
    } else if (
      (systemName.contains("CentOS") || systemName.contains("Oracle"))
      && systemVersion.startsWith("8")
    ) {
      new SharedLibraryLoaderCentos8
    } else if (
      (systemName.contains("CentOS") || systemName.contains("Oracle"))
      && systemVersion.startsWith("7")
    ) {
      new SharedLibraryLoaderCentos7
    } else if (systemName.contains("Alibaba Cloud Linux") && systemVersion.startsWith("3")) {
      new SharedLibraryLoaderCentos8
    } else if (systemName.contains("Alibaba Cloud Linux") && systemVersion.startsWith("2")) {
      new SharedLibraryLoaderCentos7
    } else if (systemName.contains("Anolis") && systemVersion.startsWith("8")) {
      new SharedLibraryLoaderCentos8
    } else if (systemName.contains("Anolis") && systemVersion.startsWith("7")) {
      new SharedLibraryLoaderCentos7
    } else if (system.contains("tencentos") && system.contains("2.4")) {
      new SharedLibraryLoaderCentos7
    } else if (system.contains("tencentos") && system.contains("3.2")) {
      new SharedLibraryLoaderCentos8
    } else if (systemName.contains("Red Hat") && systemVersion.startsWith("9")) {
      new SharedLibraryLoaderCentos9
    } else if (systemName.contains("Red Hat") && systemVersion.startsWith("8")) {
      new SharedLibraryLoaderCentos8
    } else if (systemName.contains("Red Hat") && systemVersion.startsWith("7")) {
      new SharedLibraryLoaderCentos7
    } else if (systemName.contains("Debian") && systemVersion.startsWith("11")) {
      new SharedLibraryLoaderDebian11
    } else if (systemName.contains("Debian") && systemVersion.startsWith("12")) {
      new SharedLibraryLoaderDebian12
    } else {
      throw new GlutenException(
        s"Found unsupported OS($systemName, $systemVersion)! Currently, Gluten's Velox backend" +
          " only supports Ubuntu 20.04/22.04, CentOS 7/8, Oracle 7/8" +
          "Alibaba Cloud Linux 2/3 & Anolis 7/8, tencentos 2.4/3.2, RedHat 7/8, " +
          "Debian 11/12.")
    }
  }
}
