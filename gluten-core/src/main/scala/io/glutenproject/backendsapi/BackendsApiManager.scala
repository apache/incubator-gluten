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

  private var glutenBackendName: Option[String] = None
  private var initializerApiInstance: Option[IInitializerApi] = None
  private var iteratorApiInstance: Option[IIteratorApi] = None
  private var sparkPlanExecApiInstance: Option[ISparkPlanExecApi] = None
  private var transformerApiInstance: Option[ITransformerApi] = None

  /**
   * Automatically detect the backend api.
   * @return
   */
  def initialize(): String = synchronized {
    initializeInternal()
  }

  /**
   * Initialize all backends api.
   */
  private def initializeInternal(): String = {
    val discoveredBackends = JavaConverters.iterableAsScalaIterable(
      ServiceLoader.load(classOf[Backend])).toSeq
    if (discoveredBackends.isEmpty) {
      throw new IllegalStateException("Backend implementation not discovered from JVM classpath")
    }
    if (discoveredBackends.size != 1) {
      throw new IllegalStateException(
        "More than one Backend implementation discovered from JVM classpath")
    }
    val backend = discoveredBackends.head
    glutenBackendName = Some(backend.name())
    initializerApiInstance = Some(backend.initializerApi())
    iteratorApiInstance = Some(backend.iteratorApi())
    sparkPlanExecApiInstance = Some(backend.sparkPlanExecApi())
    transformerApiInstance = Some(backend.transformerApi())
    glutenBackendName.getOrElse(throw new IllegalStateException())
  }

  def getBackendName: String = {
    glutenBackendName.getOrElse(throw new IllegalStateException("Backend not initialized"))
  }

  def getInitializerApiInstance: IInitializerApi = {
    initializerApiInstance.getOrElse(
      throw new IllegalStateException("IInitializerApi not instantiated"))
  }

  def getIteratorApiInstance: IIteratorApi = {
    iteratorApiInstance.getOrElse(throw new IllegalStateException("IIteratorApi not instantiated"))
  }

  def getSparkPlanExecApiInstance: ISparkPlanExecApi = {
    sparkPlanExecApiInstance.getOrElse(
      throw new IllegalStateException("ISparkPlanExecApi not instantiated"))
  }

  def getTransformerApiInstance: ITransformerApi = {
    transformerApiInstance.getOrElse(
      throw new IllegalStateException("ITransformerApi not instantiated"))
  }
}
