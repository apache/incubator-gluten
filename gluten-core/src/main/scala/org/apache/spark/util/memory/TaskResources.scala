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

import io.glutenproject.memory.TaskMemoryMetrics
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.memory.TaskResources.inSparkTask
import org.apache.spark.util.{TaskCompletionListener, TaskFailureListener}
import shaded.parquet.it.unimi.dsi.fastutil.longs.LongComparators

import java.util
import java.util.{Collections, Comparator, UUID}
import java.util.concurrent.atomic.AtomicLong
import scala.collection.JavaConverters._

object TaskResources extends Logging {
  // And open java assert mode to get memory stack
  val DEBUG: Boolean = {
    SQLConf.get
      .getConfString("spark.gluten.sql.memory.debug", "true").toBoolean
  }
  val ACCUMULATED_LEAK_BYTES = new AtomicLong(0L)

  private val RESOURCE_REGISTRIES =
    new java.util.IdentityHashMap[TaskContext, TaskMemoryResourceRegistry]()

  def getLocalTaskContext(): TaskContext = {
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
        tc.addTaskFailureListener(
          // in case of crashing in task completion listener, errors may be swallowed
          new TaskFailureListener {
            override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
              // TODO:
              // The general duty of printing error message should not reside in memory module
              logError(s"Task ${context.taskAttemptId()} failed by error: ", error)
            }
          })
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

  def addRecycler(prio: Long)(f: => Unit): Unit = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Not in a Spark task")
    }
    addAnonymousResourceManager(new TaskResourceManager {
      override def release(): Unit = f
      override def priority(): Long = prio
    })
  }

  def addAnonymousResourceManager(manager: TaskResourceManager): Unit = {
    getOrCreateTaskMemoryResourceRegistry().addManager(UUID.randomUUID().toString, manager)
  }

  def isResourceManagerRegistered(id: String): Boolean = {
    getOrCreateTaskMemoryResourceRegistry().isManagerRegistered(id)
  }

  def getResourceManager[T <: TaskResourceManager](id: String): T = {
    getOrCreateTaskMemoryResourceRegistry().getManager(id)
  }

  def addResourceManager(id: String, manager: TaskResourceManager): Unit = {
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

  private val managers = new java.util.LinkedHashMap[String, TaskResourceManager]()

  private val managersPriorityMapping =
    new java.util.HashMap[Long, java.util.List[TaskResourceManager]]()

  private def addManager0(id: String, resource: TaskResourceManager): Unit = {
    managers.put(id, resource)
    if (!managersPriorityMapping.containsKey(resource.priority())) {
      managersPriorityMapping.put(resource.priority(), new util.ArrayList[TaskResourceManager]())
    }
    val list = managersPriorityMapping.get(resource.priority())
    list.add(resource)
  }

  private[memory] def releaseAll(): Unit = {
    val managerTable: java.util.List[
      java.util.Map.Entry[Long, java.util.List[TaskResourceManager]]] =
      new java.util.ArrayList(managersPriorityMapping.entrySet())
    Collections.sort(managerTable,
      (o1: java.util.Map.Entry[Long, java.util.List[TaskResourceManager]],
       o2: java.util.Map.Entry[Long, java.util.List[TaskResourceManager]]) => {
        val diff = o2.getKey - o1.getKey // descending by priority
        if (diff > 0) 1 else if (diff < 0) -1 else 0
      })
    managerTable.forEach { e =>
      e.getValue.asScala.reverse.foreach(m => try { // LIFO
        m.release()
      } catch {
        case e: Throwable =>
          logWarning("Failed to call release() on resource manager instance", e)
      })
    }
  }

  private[memory] def addManager(id: String, resource: TaskResourceManager): Unit = {
    if (managers.containsKey(id)) {
      throw new IllegalArgumentException(
        String.format("TaskResourceManager with ID %s is already registered", id))
    }
    addManager0(id, resource)
  }

  private[memory] def isManagerRegistered(id: String): Boolean = {
    managers.containsKey(id)
  }

  private[memory] def getManager[T <: TaskResourceManager](id: String): T = {
    if (!managers.containsKey(id)) {
      throw new IllegalArgumentException(
        String.format("TaskResourceManager with ID %s is not registered", id))
    }
    managers.get(id).asInstanceOf[T]
  }

  private[memory] def getSharedMetrics(): TaskMemoryMetrics = {
    sharedMetrics
  }
}
