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
package org.apache.spark.util

object GlutenShutdownManager {

  private def isJava8: Boolean = System.getProperty("java.version").equals("8")

  def addHook(hook: () => Unit): AnyRef = {
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.DEFAULT_SHUTDOWN_PRIORITY)(hook)
  }

  // Java 11+ registers the Native Libraries for
  // cleaning while loading in java/lang/ClassLoader.java#L2438
  def addHookForLibUnloading(hook: () => Unit): AnyRef = {
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.SPARK_CONTEXT_SHUTDOWN_PRIORITY) {
      if (isJava8) hook else () => Unit
    }
  }

  def addHookForTempDirRemoval(hook: () => Unit): AnyRef = {
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY)(hook)
  }
}
