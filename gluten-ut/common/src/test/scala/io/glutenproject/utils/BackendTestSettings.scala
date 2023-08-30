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
import io.glutenproject.test.TestStats

import java.util

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

abstract class BackendTestSettings {

  private val enabledSuites: java.util.Map[String, TestNameFilters] = new util.HashMap()

  protected def enableSuite[T: ClassTag]: TestNameFilters = {
    val suiteName = implicitly[ClassTag[T]].runtimeClass.getCanonicalName
    if (enabledSuites.containsKey(suiteName)) {
      throw new IllegalArgumentException("Duplicated suite name: " + suiteName)
    }
    val filters = new TestNameFilters
    enabledSuites.put(suiteName, filters)
    filters
  }

  private[utils] def shouldRun(suiteName: String, testName: String): Boolean = {
    if (!enabledSuites.containsKey(suiteName)) {
      return false
    }

    val filters = enabledSuites.get(suiteName)

    val inclusion = filters.inclusion.asScala
    val exclusion = filters.exclusion.asScala

    if (inclusion.isEmpty && exclusion.isEmpty) {
      // default to run all cases under this suite
      return true
    }

    if (inclusion.nonEmpty && exclusion.nonEmpty) {
      // error
      throw new IllegalStateException(
        s"Do not use include and exclude conditions on the same test case: $suiteName:$testName")
    }

    if (inclusion.nonEmpty) {
      // include mode
      val isIncluded = inclusion.exists(_.isIncluded(testName))
      return isIncluded
    }

    if (exclusion.nonEmpty) {
      // exclude mode
      val isExcluded = exclusion.exists(_.isExcluded(testName))
      return !isExcluded
    }

    throw new IllegalStateException("Unreachable code")
  }

  final protected class TestNameFilters {
    private[utils] val inclusion: util.List[IncludeBase] = new util.ArrayList()
    private[utils] val exclusion: util.List[ExcludeBase] = new util.ArrayList()

    private val TEMP_DISABLE_ALL_TAG = "temp_disable_all"

    def include(testNames: String*): TestNameFilters = {
      inclusion.add(Include(testNames: _*))
      this
    }
    def exclude(testNames: String*): TestNameFilters = {
      exclusion.add(Exclude(testNames: _*))
      this
    }
    def includeByPrefix(prefixes: String*): TestNameFilters = {
      inclusion.add(IncludeByPrefix(prefixes: _*))
      this
    }
    def excludeByPrefix(prefixes: String*): TestNameFilters = {
      exclusion.add(ExcludeByPrefix(prefixes: _*))
      this
    }

    def excludeAll(reason: String): TestNameFilters = {
      exclusion.clear()
      inclusion.clear()
      inclusion.add(Include(Seq(TEMP_DISABLE_ALL_TAG, reason).mkString(" ")))
      this
    }
  }

  protected trait IncludeBase {
    def isIncluded(testName: String): Boolean
  }
  protected trait ExcludeBase {
    def isExcluded(testName: String): Boolean
  }
  private case class Include(testNames: String*) extends IncludeBase {
    val nameSet: Set[String] = Set(testNames: _*)
    override def isIncluded(testName: String): Boolean = nameSet.contains(testName)
  }
  private case class Exclude(testNames: String*) extends ExcludeBase {
    val nameSet: Set[String] = Set(testNames: _*)
    override def isExcluded(testName: String): Boolean = nameSet.contains(testName)
  }
  private case class IncludeByPrefix(prefixes: String*) extends IncludeBase {
    override def isIncluded(testName: String): Boolean = {
      if (prefixes.exists(prefix => testName.startsWith(prefix))) {
        return true
      }
      false
    }
  }
  private case class ExcludeByPrefix(prefixes: String*) extends ExcludeBase {
    override def isExcluded(testName: String): Boolean = {
      if (prefixes.exists(prefix => testName.startsWith(prefix))) {
        return true
      }
      false
    }
  }
}

object BackendTestSettings {
  val instance: BackendTestSettings = BackendsApiManager.getBackendName match {
    case GlutenConfig.GLUTEN_CLICKHOUSE_BACKEND =>
      // scalastyle:off classforname
      Class
        .forName("io.glutenproject.utils.clickhouse.ClickHouseTestSettings")
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[BackendTestSettings]
    case GlutenConfig.GLUTEN_VELOX_BACKEND =>
      Class
        .forName("io.glutenproject.utils.velox.VeloxTestSettings")
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[BackendTestSettings]
    // scalastyle:on classforname
    case other =>
      throw new IllegalStateException(other)
  }

  def shouldRun(suiteName: String, testName: String): Boolean = {
    val v = instance.shouldRun(suiteName, testName: String)

    if (!v) {
      TestStats.addIgnoreCaseName(testName)
    }

    v
  }
}
