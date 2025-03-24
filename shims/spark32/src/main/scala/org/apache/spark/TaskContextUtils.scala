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
package org.apache.spark

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.memory.{TaskMemoryManager, UnifiedMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.storage.BlockManagerUtils

import java.util.Properties

import scala.collection.JavaConverters._

object TaskContextUtils {
  def createTestTaskContext(properties: Properties): TaskContext = {
    val conf = new SparkConf()
    conf.setAll(properties.asScala)
    val memoryManager = UnifiedMemoryManager(conf, 1)
    BlockManagerUtils.setTestMemoryStore(conf, memoryManager, isDriver = false)
    new TaskContextImpl(
      -1,
      -1,
      -1,
      -1L,
      -1,
      new TaskMemoryManager(memoryManager, -1L),
      properties,
      MetricsSystem.createMetricsSystem("GLUTEN_UNSAFE", conf),
      TaskMetrics.empty,
      Map.empty
    )
  }
}
