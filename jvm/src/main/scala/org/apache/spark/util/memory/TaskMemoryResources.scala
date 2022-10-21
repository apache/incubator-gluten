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

package org.apache.spark.util.memory

import java.util.concurrent.atomic.AtomicLong
import java.util.UUID

import scala.collection.JavaConverters._

import io.glutenproject.memory.TaskMemoryMetrics

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.config.MEMORY_OFFHEAP_SIZE
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.util.memory.TaskMemoryResources._

object TaskMemoryResources {
  val DEBUG: Boolean = {
    SQLConf.get
      .getConfString("spark.gluten.sql.memory.debug", "false").toBoolean
  }
  val ACCUMULATED_LEAK_BYTES = new AtomicLong(0L)

  val OFFHEAP_SIZE: Long = SparkEnv.get.conf.get(MEMORY_OFFHEAP_SIZE)

  private val RESOURCE_REGISTRIES =
    new java.util.IdentityHashMap[TaskContext, TaskMemoryResourceRegistry]()

  private[memory] def getLocalTaskContext(): TaskContext = {
    TaskContext.get()
  }

  def inSparkTask(): Boolean = {
    TaskContext.get() != null
  }

  def getSparkMemoryManager(): TaskMemoryManager = {
    getLocalTaskContext().taskMemoryManager()
  }

  private[memory] def getOrCreateTaskMemoryResourceRegistry(): TaskMemoryResourceRegistry = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Not in a Spark task")
    }
    val tc = getLocalTaskContext()
    RESOURCE_REGISTRIES.synchronized {

      if (!RESOURCE_REGISTRIES.containsKey(tc)) {
        RESOURCE_REGISTRIES.put(tc, new TaskMemoryResourceRegistry)
        tc.addTaskCompletionListener(
          new TaskCompletionListener {
            override def onTaskCompletion(context: TaskContext): Unit = {
              RESOURCE_REGISTRIES.synchronized {
                val registry = RESOURCE_REGISTRIES.remove(context)
                registry.releaseAll()
                context.taskMetrics().incPeakExecutionMemory(registry.getSharedMetrics().peak())
              }
            }
          })
      }

      return RESOURCE_REGISTRIES.get(tc)
    }
  }

  def addLeakSafeTaskCompletionListener[U](f: TaskContext => U): TaskContext = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Not in a Spark task")
    }
    getOrCreateTaskMemoryResourceRegistry() // initialize cleaners
    getLocalTaskContext().addTaskCompletionListener(f)
  }

  def addAnonymousResourceManager(manager: TaskMemoryResourceManager): Unit = {
    getOrCreateTaskMemoryResourceRegistry().addManager(UUID.randomUUID().toString, manager)
  }

  def isResourceManagerRegistered(id: String): Boolean = {
    getOrCreateTaskMemoryResourceRegistry().isManagerRegistered(id)
  }

  def getResourceManager[T <: TaskMemoryResourceManager](id: String): T = {
    getOrCreateTaskMemoryResourceRegistry().getManager(id)
  }

  def addResourceManager(id: String, manager: TaskMemoryResourceManager): Unit = {
    getOrCreateTaskMemoryResourceRegistry().addManager(id, manager)
  }

  def getSharedMetrics(): TaskMemoryMetrics = {
    getOrCreateTaskMemoryResourceRegistry().getSharedMetrics()
  }
}

class TaskMemoryResourceRegistry extends Logging {
  if (!inSparkTask()) {
    throw new IllegalStateException(
      "Creating TaskMemoryResourceRegistry instance out of Spark task")
  }

  private val sharedMetrics = new TaskMemoryMetrics()

  private val managers = new java.util.LinkedHashMap[String, TaskMemoryResourceManager]()

  private[memory] def releaseAll(): Unit = {
    managers.values().asScala.toArray.reverse.foreach(m => try {
      m.release()
    } catch {
      case e: Throwable =>
        logWarning("Failed to call release() on resource manager instance", e)
    })
  }

  private[memory] def addManager(id: String, resource: TaskMemoryResourceManager): Unit = {
    if (managers.containsKey(id)) {
      throw new IllegalArgumentException(
        String.format("TaskMemoryResourceManager with ID %s is already registered", id))
    }
    managers.put(id, resource)
  }

  private[memory] def isManagerRegistered(id: String): Boolean = {
    managers.containsKey(id)
  }

  private[memory] def getManager[T <: TaskMemoryResourceManager](id: String): T = {
    if (!managers.containsKey(id)) {
      throw new IllegalArgumentException(
        String.format("TaskMemoryResourceManager with ID %s is not registered", id))
    }
    managers.get(id).asInstanceOf[T]
  }

  private[memory] def getSharedMetrics(): TaskMemoryMetrics = {
    sharedMetrics
  }
}
