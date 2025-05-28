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
package org.apache.gluten.monitor

import org.apache.gluten.backendsapi.velox.VeloxBackend
import org.apache.gluten.component.Component
import org.apache.gluten.config.VeloxConfig.MEMORY_DUMP_ON_EXIT
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.api.plugin.PluginContext
import org.apache.spark.internal.Logging
import org.apache.spark.util.SparkShutdownManagerUtil

class VeloxMonitorComponent extends Component with Logging {

  /** Base information. */
  override def name(): String = "velox-monitor"

  override def buildInfo(): Component.BuildInfo =
    Component.BuildInfo("VeloxMonitor", "N/A", "N/A", "N/A")

  override def onExecutorStart(pc: PluginContext): Unit = {
    val memoryDumpOnExit =
      pc.conf().get(MEMORY_DUMP_ON_EXIT.key, MEMORY_DUMP_ON_EXIT.defaultValueString).toBoolean
    if (memoryDumpOnExit) {
      SparkShutdownManagerUtil.addHook(
        () => {
          logInfo("MemoryDumpOnExit monitor triggered, dumping memory profile.")
          VeloxMemoryProfiler.dump()
          logInfo("MemoryDumpOnExit monitor completed.")
        })
    }
  }

  override def dependencies(): Seq[Class[_ <: Component]] = classOf[VeloxBackend] :: Nil

  override def injectRules(injector: Injector): Unit = {}
}
