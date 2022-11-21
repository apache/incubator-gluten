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

package io.glutenproject.backendsapi

import java.util.ServiceLoader

import scala.collection.JavaConverters

object BackendsApiManager {

  private case class Wrapper(
      glutenBackendName: String,
      initializerApiInstance: IInitializerApi,
      iteratorApiInstance: IIteratorApi,
      sparkPlanExecApiInstance: ISparkPlanExecApi,
      transformerApiInstance: ITransformerApi,
      settings: BackendSettings)

  private lazy val manager: Wrapper = initializeInternal()

  /** Initialize all backends api. */
  private def initializeInternal(): Wrapper = {
    val discoveredBackends =
      JavaConverters.iterableAsScalaIterable(ServiceLoader.load(classOf[Backend])).toSeq
    if (discoveredBackends.isEmpty) {
      throw new IllegalStateException("Backend implementation not discovered from JVM classpath")
    }
    if (discoveredBackends.size != 1) {
      throw new IllegalStateException(
        s"More than one Backend implementation discovered from JVM classpath: " +
          s"${discoveredBackends.map(_.name()).toList}")
    }
    val backend = discoveredBackends.head
    Wrapper(
      backend.name(),
      backend.initializerApi(),
      backend.iteratorApi(),
      backend.sparkPlanExecApi(),
      backend.transformerApi(),
      backend.settings())
  }

  /**
   * Automatically detect the backend api.
   * @return
   */
  def initialize(): String = {
    getBackendName
  }

  def getBackendName: String = {
    manager.glutenBackendName
  }

  def getInitializerApiInstance: IInitializerApi = {
    manager.initializerApiInstance
  }

  def getIteratorApiInstance: IIteratorApi = {
    manager.iteratorApiInstance
  }

  def getSparkPlanExecApiInstance: ISparkPlanExecApi = {
    manager.sparkPlanExecApiInstance
  }

  def getTransformerApiInstance: ITransformerApi = {
    manager.transformerApiInstance
  }

  def getSettings: BackendSettings = {
    manager.settings
  }
}
