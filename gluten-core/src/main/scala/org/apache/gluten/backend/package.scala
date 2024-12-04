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

package org.apache.gluten

import org.apache.spark.internal.Logging

import java.util.ServiceLoader
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

package object backend extends Logging {
  private[backend] val allComponentsLoaded: AtomicBoolean = new AtomicBoolean(false)

  private[backend] def ensureAllComponentsRegistered(): Unit = {
    if (!allComponentsLoaded.compareAndSet(false, true)) {
      return
    }

    // Load all components in classpath.
    val discoveredBackends = ServiceLoader.load(classOf[Backend]).asScala
    val discoveredComponents = ServiceLoader.load(classOf[Component]).asScala
    val all = discoveredBackends ++ discoveredComponents

    // Register all components.
    all.foreach(_.ensureRegistered())

    // Output log so user could view the component loading order.
    val components = Component.sorted()
    logInfo(s"Components registered within order: ${components.mkString(", ")}")
  }
}
