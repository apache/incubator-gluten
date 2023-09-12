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
package io.glutenproject

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.resource.ResourceInformation

import com.codahale.metrics.MetricRegistry

import java.util

class GlutenPluginSuite extends SparkFunSuite {

  test("Init GlutenExecutorPlugin without enabled gluten off-heap") {
    val glutenPlugin = new GlutenPlugin()
    val conf = new SparkConf()
      .set(GlutenConfig.GLUTEN_ENABLED.key, "true")
      .set(GlutenConfig.GLUTEN_OFFHEAP_ENABLED, "false")
    assertThrows[IllegalArgumentException] {
      glutenPlugin
        .executorPlugin()
        .init(
          new MockPluginContext(conf = conf, execId = "1", hostName = "localhost"),
          util.Map.of())
    }
    // [GLUTEN-3114] Check whether enabled gluten when init GlutenExecutorPlugin
    conf.set(GlutenConfig.GLUTEN_ENABLED.key, "false")
    glutenPlugin
      .executorPlugin()
      .init(new MockPluginContext(conf = conf, execId = "1", hostName = "localhost"), util.Map.of())
  }
}

private class MockPluginContext(
    metricRegistry: MetricRegistry = null,
    conf: SparkConf,
    execId: String,
    hostName: String,
    resources: util.Map[String, ResourceInformation] = null)
  extends PluginContext {
  override def metricRegistry(): MetricRegistry = metricRegistry

  override def conf(): SparkConf = conf

  override def executorID(): String = execId

  override def hostname(): String = hostName

  override def resources(): util.Map[String, ResourceInformation] = resources

  override def send(message: Any): Unit = {
    // TODO: implement
  }

  override def ask(message: Any): AnyRef = {
    // TODO: implement
    null
  }
}
