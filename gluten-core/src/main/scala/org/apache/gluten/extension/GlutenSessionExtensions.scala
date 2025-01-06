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
package org.apache.gluten.extension

import org.apache.gluten.component.Component
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension.injector.Injector

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions

private[gluten] class GlutenSessionExtensions
  extends (SparkSessionExtensions => Unit)
  with Logging {
  import GlutenSessionExtensions._
  override def apply(exts: SparkSessionExtensions): Unit = {
    val injector = new Injector(exts)
    injector.control.disableOn {
      session =>
        val glutenEnabledGlobally = session.conf
          .get(GlutenConfig.GLUTEN_ENABLED_KEY, GlutenConfig.GLUTEN_ENABLED_BY_DEFAULT.toString)
          .toBoolean
        val disabled = !glutenEnabledGlobally
        logDebug(s"Gluten is disabled by variable: glutenEnabledGlobally: $glutenEnabledGlobally")
        disabled
    }
    injector.control.disableOn {
      session =>
        val glutenEnabledForThread =
          Option(session.sparkContext.getLocalProperty(GLUTEN_ENABLE_FOR_THREAD_KEY))
            .forall(_.toBoolean)
        val disabled = !glutenEnabledForThread
        logDebug(s"Gluten is disabled by variable: glutenEnabledForThread: $glutenEnabledForThread")
        disabled
    }
    // Components should override Backend's rules. Hence, reversed injection order is applied.
    Component.sorted().reverse.foreach(_.injectRules(injector))
    injector.inject()
  }
}

object GlutenSessionExtensions {
  val GLUTEN_SESSION_EXTENSION_NAME: String = classOf[GlutenSessionExtensions].getCanonicalName
  val GLUTEN_ENABLE_FOR_THREAD_KEY: String = "gluten.enabledForCurrentThread"
}
