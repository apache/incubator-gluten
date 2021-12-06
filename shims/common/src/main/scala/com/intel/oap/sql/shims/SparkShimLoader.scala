/*
 * Copyright 2020 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.oap.sql.shims

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.spark.{SPARK_VERSION}
import org.apache.spark.internal.Logging

object SparkShimLoader extends Logging {
  private var sparkShims: SparkShims = null
  private var sparkShimProviderClass: String = null

  def getSparkShims: SparkShims = {
    if (sparkShims == null) {
      val provider = getSparkShimProvider()
      sparkShims = provider.createShim
    }
    sparkShims
  }

  def getSparkVersion: String = {
      SPARK_VERSION
  }

  def setSparkShimProviderClass(providerClass: String): Unit = {
    sparkShimProviderClass = providerClass
  }
  
  private def loadSparkShimProvider(): SparkShimProvider = {
    // Match and load Shim provider for current Spark version.
    val sparkVersion = getSparkVersion
    logInfo(s"Loading Spark Shims for version: $sparkVersion")

    //Load and filter the providers based on version
    val shimProviders = ServiceLoader.load(classOf[SparkShimProvider]).asScala.filter(_.matches(sparkVersion))
    if (shimProviders.size > 1) {
      throw new IllegalStateException(s"More than one SparkShimProvider found: $shimProviders")
    }

    val shimProvider = shimProviders.headOption match {
      case Some(shimProvider) => shimProvider
      case None =>
        throw new IllegalStateException(s"No Spark Shim Provider found for $sparkVersion")
    }
    logInfo(s"Using Shim provider: $shimProviders")
    shimProvider
  }

  private def getSparkShimProvider(): SparkShimProvider = {
    if (sparkShimProviderClass != null) {
      logInfo(s"Using Spark Shim Provider specified by $sparkShimProviderClass. ")
      val providerClass = Class.forName(sparkShimProviderClass)
      val providerConstructor = providerClass.getConstructor()
      providerConstructor.newInstance().asInstanceOf[SparkShimProvider]
    } else {
      loadSparkShimProvider()
    }
  }
}
