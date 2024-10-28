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
package org.apache.gluten.backend

import org.apache.gluten.extension.columnar.transition.{Convention, ConventionFunc}
import org.apache.gluten.extension.injector.RuleInjector

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.PluginContext

import java.util.ServiceLoader

import scala.collection.JavaConverters

trait Backend {
  import Backend._

  /** Base information. */
  def name(): String
  def buildInfo(): BuildInfo

  /** Spark listeners. */
  def onDriverStart(sc: SparkContext, pc: PluginContext): Unit = {}
  def onDriverShutdown(): Unit = {}
  def onExecutorStart(pc: PluginContext): Unit = {}
  def onExecutorShutdown(): Unit = {}

  /** The columnar-batch type this backend is by default using. */
  def defaultBatchType: Convention.BatchType

  /**
   * Overrides [[org.apache.gluten.extension.columnar.transition.ConventionFunc]] Gluten is using to
   * determine the convention (its row-based processing / columnar-batch processing support) of a
   * plan with a user-defined function that accepts a plan then returns convention type it outputs,
   * and input conventions it requires.
   */
  def convFuncOverride(): ConventionFunc.Override = ConventionFunc.Override.Empty

  /** Query planner rules. */
  def injectRules(injector: RuleInjector): Unit
}

object Backend {
  private val backend: Backend = {
    val discoveredBackends =
      JavaConverters.iterableAsScalaIterable(ServiceLoader.load(classOf[Backend])).toList
    discoveredBackends match {
      case Nil =>
        throw new IllegalStateException("Backend implementation not discovered from JVM classpath")
      case head :: Nil =>
        head
      case backends =>
        val backendNames = backends.map(_.name())
        throw new IllegalStateException(
          s"More than one Backend implementation discovered from JVM classpath: $backendNames")
    }
  }

  def get(): Backend = {
    backend
  }

  case class BuildInfo(name: String, branch: String, revision: String, revisionTime: String)
}
