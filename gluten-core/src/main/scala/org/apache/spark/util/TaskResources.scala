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

import org.apache.spark.{TaskContext, TaskFailedReason, TaskKilledException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

import _root_.io.glutenproject.backendsapi.BackendsApiManager
import _root_.io.glutenproject.memory.MemoryUsage
import _root_.io.glutenproject.utils.TaskListener

import java.util
import java.util.{Collections, UUID}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._

object TaskResources extends TaskListener with Logging {
  // And open java assert mode to get memory stack
  val DEBUG: Boolean = {
    SQLConf.get
      .getConfString("spark.gluten.sql.memory.debug", "true")
      .toBoolean
  }
  val ACCUMULATED_LEAK_BYTES = new AtomicLong(0L)

  private val RESOURCE_REGISTRIES =
    new java.util.IdentityHashMap[TaskContext, TaskResourceRegistry]()

  def getLocalTaskContext(): TaskContext = {
    TaskContext.get()
  }

  def inSparkTask(): Boolean = {
    TaskContext.get() != null
  }

  private def getTaskResourceRegistry(): TaskResourceRegistry = {
    if (!inSparkTask()) {
      logWarning(
        "Using the fallback instance of TaskResourceRegistry. " +
          "This should only happen when call is not from Spark task.")
      throw new IllegalStateException("Found a caller not in Spark task scope.")
    }
    val tc = getLocalTaskContext()
    RESOURCE_REGISTRIES.synchronized {
      if (!RESOURCE_REGISTRIES.containsKey(tc)) {
        throw new IllegalStateException(
          "" +
            "TaskResourceRegistry is not initialized, please ensure TaskResources " +
            "is added to GlutenExecutorPlugin's task listener list")
      }
      return RESOURCE_REGISTRIES.get(tc)
    }
  }

  def addRecycler(name: String, prio: Long)(f: => Unit): Unit = {
    addAnonymousResource(new TaskResource {
      override def release(): Unit = f

      override def priority(): Long = prio

      override def resourceName(): String = name
    })
  }

  def addResource[T <: TaskResource](id: String, resource: T): T = {
    getTaskResourceRegistry().addResource(id, resource)
  }

  def addResourceIfNotRegistered[T <: TaskResource](id: String, factory: () => T): T = {
    getTaskResourceRegistry().addResourceIfNotRegistered(id, factory)
  }

  def addAnonymousResource[T <: TaskResource](resource: T): T = {
    getTaskResourceRegistry().addResource(UUID.randomUUID().toString, resource)
  }

  def isResourceRegistered(id: String): Boolean = {
    getTaskResourceRegistry().isResourceRegistered(id)
  }

  def getResource[T <: TaskResource](id: String): T = {
    getTaskResourceRegistry().getResource(id)
  }

  def getSharedUsage(): MemoryUsage = {
    getTaskResourceRegistry().getSharedUsage()
  }

  override def onTaskStart(): Unit = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Not in a Spark task")
    }
    val tc = getLocalTaskContext()
    RESOURCE_REGISTRIES.synchronized {
      if (RESOURCE_REGISTRIES.containsKey(tc)) {
        throw new IllegalStateException(
          "TaskResourceRegistry is already initialized, this should not happen")
      }
      val registry = new TaskResourceRegistry
      RESOURCE_REGISTRIES.put(tc, registry)
      tc.addTaskFailureListener(
        // in case of crashing in task completion listener, errors may be swallowed
        new TaskFailureListener {
          override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
            // TODO:
            // The general duty of printing error message should not reside in memory module
            error match {
              case e: TaskKilledException if e.reason == "another attempt succeeded" =>
              case _ => logError(s"Task ${context.taskAttemptId()} failed by error: ", error)
            }
          }
        })
      tc.addTaskCompletionListener(new TaskCompletionListener {
        override def onTaskCompletion(context: TaskContext): Unit = {
          RESOURCE_REGISTRIES.synchronized {
            if (!RESOURCE_REGISTRIES.containsKey(tc)) {
              throw new IllegalStateException(
                "TaskResourceRegistry is not initialized, this should not happen")
            }
            val registry = RESOURCE_REGISTRIES.remove(context)
            registry.releaseAll()
            context.taskMetrics().incPeakExecutionMemory(registry.getSharedUsage().peak())
          }
        }
      })
    }
    // Register resources from context API
    val resourceFactories = BackendsApiManager.getContextApiInstance.taskResourceFactories()
    for (factory <- resourceFactories) {
      addAnonymousResource(factory.apply())
    }
  }

  private def onTaskExit(): Unit = {
    // no-op
  }

  override def onTaskSucceeded(): Unit = {
    onTaskExit()
  }

  override def onTaskFailed(failureReason: TaskFailedReason): Unit = {
    onTaskExit()
  }
}

// thread safe
class TaskResourceRegistry extends Logging {
  private val sharedUsage = new MemoryUsage()
  private val resources = new java.util.LinkedHashMap[String, TaskResource]()
  private val resourcesPriorityMapping =
    new java.util.HashMap[Long, java.util.List[TaskResource]]()

  private def addResource0(id: String, resource: TaskResource): Unit = synchronized {
    resources.put(id, resource)
    if (!resourcesPriorityMapping.containsKey(resource.priority())) {
      resourcesPriorityMapping.put(resource.priority(), new util.ArrayList[TaskResource]())
    }
    val list = resourcesPriorityMapping.get(resource.priority())
    list.add(resource)
  }

  /** Release all managed resources according to priority and reversed order */
  private[util] def releaseAll(): Unit = synchronized {
    val resourceTable: java.util.List[java.util.Map.Entry[Long, java.util.List[TaskResource]]] =
      new java.util.ArrayList(resourcesPriorityMapping.entrySet())
    Collections.sort(
      resourceTable,
      (
          o1: java.util.Map.Entry[Long, java.util.List[TaskResource]],
          o2: java.util.Map.Entry[Long, java.util.List[TaskResource]]) => {
        val diff = o2.getKey - o1.getKey // descending by priority
        if (diff > 0) 1 else if (diff < 0) -1 else 0
      }
    )
    resourceTable.forEach {
      e =>
        e.getValue.asScala.reverse.foreach(
          m =>
            try { // LIFO
              // logWarning(s"Prepare release ${m.resourceName()}")
              m.release()
            } catch {
              case e: Throwable =>
                logWarning("Failed to call release() on resource instance", e)
            })
    }
  }

  private[util] def addResourceIfNotRegistered[T <: TaskResource](id: String, factory: () => T): T =
    synchronized {
      if (resources.containsKey(id)) {
        return resources.get(id).asInstanceOf[T]
      }
      val resource = factory.apply()
      addResource0(id, resource)
      resource
    }

  private[util] def addResource[T <: TaskResource](id: String, resource: T): T = synchronized {
    if (resources.containsKey(id)) {
      throw new IllegalArgumentException(
        String.format("TaskResource with ID %s is already registered", id))
    }
    addResource0(id, resource)
    resource
  }

  private[util] def isResourceRegistered(id: String): Boolean = synchronized {
    resources.containsKey(id)
  }

  private[util] def getResource[T <: TaskResource](id: String): T = synchronized {
    if (!resources.containsKey(id)) {
      throw new IllegalArgumentException(
        String.format("TaskResource with ID %s is not registered", id))
    }
    resources.get(id).asInstanceOf[T]
  }

  private[util] def getSharedUsage(): MemoryUsage = synchronized {
    sharedUsage
  }
}
