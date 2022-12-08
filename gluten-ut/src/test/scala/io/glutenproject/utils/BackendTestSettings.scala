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

package io.glutenproject.utils

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.utils.clickhouse.ClickHouseTestSettings
import io.glutenproject.utils.velox.VeloxTestSettings

import java.util
import scala.reflect.ClassTag

abstract class BackendTestSettings {
  protected val INCLUDE_ONLY: Int = 1
  protected val EXCLUDE_ONLY: Int = 2

  private val enabledSuites: java.util.Map[String, SuiteAction] = new util.HashMap()

  // default to exclude no cases (run all tests under this suite)
  protected def enableSuite[T: ClassTag](action: SuiteAction = ExcludeOnly())(
    implicit suiteClass: ClassTag[T]): Unit = {
    val suiteName = suiteClass.runtimeClass.getCanonicalName
    if (enabledSuites.containsKey(suiteName)) {
      throw new IllegalArgumentException("Duplicated suite name: " + suiteName)
    }
    enabledSuites.put(suiteName, action)
  }

  def shouldRun(suiteName: String, testName: String): Boolean = {
    if (!enabledSuites.containsKey(suiteName)) {
      return false
    }

    val action: SuiteAction = enabledSuites.get(suiteName)
    action match {
      case io@IncludeOnly(_ @ _*) => io.nameSet.contains(testName)
      case eo@ExcludeOnly(_ @ _*) => !eo.nameSet.contains(testName)
    }
  }

  trait SuiteAction
  case class IncludeOnly(testNames: String*) extends SuiteAction {
    val nameSet: Set[String] = Set(testNames: _*)
  }
  case class ExcludeOnly(testNames: String*) extends SuiteAction {
    val nameSet: Set[String] = Set(testNames: _*)
  }
}

object BackendTestSettings {

  val instance: BackendTestSettings = BackendsApiManager.getBackendName match {
    case GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND =>
      ClickHouseTestSettings
    case GlutenConfig.GLUTEN_VELOX_BACKEND =>
      VeloxTestSettings
    case GlutenConfig.GLUTEN_GAZELLE_BACKEND =>
      // FIXME here we reuse Velox backend's code
      VeloxTestSettings
    case other =>
      throw new IllegalStateException(other)
  }

  def shouldRun(suiteName: String, testName: String): Boolean = {
    instance.shouldRun(suiteName, testName: String)
  }
}
