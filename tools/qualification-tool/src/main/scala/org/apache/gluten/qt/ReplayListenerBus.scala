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
package org.apache.gluten.qt

import org.apache.gluten.qt.execution.{Executor, Progress, RunState}

import ReflectionUtils.{accessPrivateClass, accessPrivateMethod}

import scala.io.BufferedSource

class ReplayListenerBus {
  private val replayBus = accessPrivateClass("org.apache.spark.scheduler.ReplayListenerBus")

  def addListener(listener: SparkListener): Unit = {
    accessPrivateMethod(replayBus, "addListener", listener.getListener)
  }

  def replay(source: BufferedSource, file: String): Boolean = {
    try {
      Executor.updateState(RunState("DOWNLOADING"))
      val lines = source.getLines()
      Executor.updateState(RunState("PARSING", Progress()))
      val processedLines = lines.map {
        line =>
          Executor.incrementState()
          line
      }
      val replayEventsFilter: String => Boolean = (_: String) => true
      accessPrivateMethod(replayBus, "replay", processedLines, file, false, replayEventsFilter)
        .asInstanceOf[Boolean]
    } catch {
      case e: Throwable => throw new Exception("Failed to process file", e)
    } finally {
      source.close()
    }
  }
}
