/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.glutenproject.backendsapi

import java.util.ServiceLoader
import scala.collection.JavaConversions._

object BackendsApiManager {

  protected var iteratorApiInstance: IIteratorApi = null

  protected var sparkPlanExecApiInstance: ISparkPlanExecApi = null

  protected var transformerApiInstance: ITransformerApi = null

  /**
   * Initialize all backends api.
   *
   * @param glutenBackenLibName
   */
  def initialize(glutenBackenLibName: String): Unit = synchronized {
    // initialize IIteratorApi instance
    if (iteratorApiInstance == null) {
      val serviceLoader = ServiceLoader.load(classOf[IIteratorApi])
      for (ele <- serviceLoader) {
        if (ele.getBackendName.equalsIgnoreCase(glutenBackenLibName)) {
          iteratorApiInstance = ele
        } else if (ele.getBackendName.equalsIgnoreCase("velox")) {
          // Currently, Velox and Gazelle backends use the same api to implement.
          // Will remove this after splitting gazelle module
          iteratorApiInstance = ele
        }
      }
    }
    assert(iteratorApiInstance != null, "Can not initialize IIteratorApi instance.")
    // initialize ISparkPlanExecApi instance
    if (sparkPlanExecApiInstance == null) {
      val serviceLoader = ServiceLoader.load(classOf[ISparkPlanExecApi])

      for (ele <- serviceLoader) {
        if (ele.getBackendName.equalsIgnoreCase(glutenBackenLibName)) {
          sparkPlanExecApiInstance = ele
        } else if (ele.getBackendName.equalsIgnoreCase("velox")) {
          // Currently, Velox and Gazelle backends use the same api to implement.
          // Will remove this after splitting gazelle module
          sparkPlanExecApiInstance = ele
        }
      }
    }
    assert(sparkPlanExecApiInstance != null, "Can not initialize " + "ISparkPlanExecApi instance.")
    // initialize ITransformerApi instance
    if (transformerApiInstance == null) {
      val serviceLoader = ServiceLoader.load(classOf[ITransformerApi])
      for (ele <- serviceLoader) {
        if (ele.getBackendName.equalsIgnoreCase(glutenBackenLibName)) {
          transformerApiInstance = ele
        } else if (ele.getBackendName.equalsIgnoreCase("velox")) {
          // Currently, Velox and Gazelle backends use the same api to implement.
          // Will remove this after splitting gazelle module
          transformerApiInstance = ele
        }
      }
    }
    assert(transformerApiInstance != null, "Can not initialize ITransformerApi instance.")
  }

  def getIteratorApiInstance: IIteratorApi = {
    if (iteratorApiInstance == null)
      throw new RuntimeException("IIteratorApi instance is null.")
    iteratorApiInstance
  }

  def getSparkPlanExecApiInstance: ISparkPlanExecApi = {
    if (sparkPlanExecApiInstance == null)
      throw new RuntimeException("ISparkPlanExecApi instance is null.")
    sparkPlanExecApiInstance
  }

  def getTransformerApiInstance: ITransformerApi = {
    if (transformerApiInstance == null)
      throw new RuntimeException("ITransformerApi instance is null.")
    transformerApiInstance
  }
}
