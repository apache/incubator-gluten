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

import org.apache.commons.lang3.StringUtils

import java.util.ServiceLoader

import scala.collection.JavaConverters

object BackendsApiManager {

  protected var glutenBackenName: String = ""

  protected var iteratorApiInstance: IIteratorApi = null

  protected var sparkPlanExecApiInstance: ISparkPlanExecApi = null

  protected var transformerApiInstance: ITransformerApi = null

  /**
   * Automatically detect the backend api.
   * @return
   */
  def initialize(): String = synchronized {
    initializeInternal(null)
  }

  /**
   * Initialize all backends api by the specified lib name.
   *
   * For test.
   * @return
   */
  def initialize(glutenBackenLibName: String): String = synchronized {
    initializeInternal(glutenBackenLibName)
  }

  /**
   * Initialize all backends api.
   *
   * @param glutenBackenLibName
   */
  private def initializeInternal(glutenBackenLibName: String): String = {
    glutenBackenName = if (StringUtils.isEmpty(glutenBackenLibName)) {
      val serviceBaseLoader = JavaConverters.iterableAsScalaIterable(
        ServiceLoader.load(classOf[IBackendsApi]))
      assert((serviceBaseLoader != null) && (serviceBaseLoader.size == 1),
        "Can not load IBackendsApi.")
      serviceBaseLoader.head.getBackendName
    } else {
      glutenBackenLibName
    }

    // initialize IIteratorApi instance
    if (iteratorApiInstance == null) {
      val serviceLoader = JavaConverters.iterableAsScalaIterable(
        ServiceLoader.load(classOf[IIteratorApi]))
      assert(serviceLoader != null, "Can not initialize IIteratorApi instance.")
      for (ele <- serviceLoader) {
        if (ele.getBackendName.equalsIgnoreCase(glutenBackenName)) {
          iteratorApiInstance = ele
        }
      }
    }
    assert(iteratorApiInstance != null, "Can not initialize IIteratorApi instance.")

    // initialize ISparkPlanExecApi instance
    if (sparkPlanExecApiInstance == null) {
      val serviceLoader = JavaConverters.iterableAsScalaIterable(
        ServiceLoader.load(classOf[ISparkPlanExecApi]))
      assert(serviceLoader != null, "Can not initialize ISparkPlanExecApi instance.")
      for (ele <- serviceLoader) {
        if (ele.getBackendName.equalsIgnoreCase(glutenBackenName)) {
          sparkPlanExecApiInstance = ele
        }
      }
    }
    assert(sparkPlanExecApiInstance != null, "Can not initialize ISparkPlanExecApi instance.")

    // initialize ITransformerApi instance
    if (transformerApiInstance == null) {
      val serviceLoader = JavaConverters.iterableAsScalaIterable(
        ServiceLoader.load(classOf[ITransformerApi]))
      assert(serviceLoader != null, "Can not initialize ITransformerApi instance.")
      for (ele <- serviceLoader) {
        if (ele.getBackendName.equalsIgnoreCase(glutenBackenName)) {
          transformerApiInstance = ele
        }
      }
    }
    assert(transformerApiInstance != null, "Can not initialize ITransformerApi instance.")

    glutenBackenName
  }

  def getIteratorApiInstance: IIteratorApi = {
    if (iteratorApiInstance == null) {
      throw new RuntimeException("IIteratorApi instance is null.")
    }
    iteratorApiInstance
  }

  def getSparkPlanExecApiInstance: ISparkPlanExecApi = {
    if (sparkPlanExecApiInstance == null) {
      throw new RuntimeException("ISparkPlanExecApi instance is null.")
    }
    sparkPlanExecApiInstance
  }

  def getTransformerApiInstance: ITransformerApi = {
    if (transformerApiInstance == null) {
      throw new RuntimeException("ITransformerApi instance is null.")
    }
    transformerApiInstance
  }
}
