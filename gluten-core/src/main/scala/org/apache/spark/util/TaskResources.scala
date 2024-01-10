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

import _root_.io.glutenproject.memory.SimpleMemoryUsageRecorder
import _root_.io.glutenproject.utils.TaskListener

import java.util
import java.util.{Comparator, PriorityQueue, UUID}
import java.util.concurrent.atomic.AtomicLong

object TaskResources extends TaskListener with Logging {
  // And open java assert mode to get memory stack
  val DEBUG: Boolean = {
    SQLConf.get
      .getConfString("spark.gluten.sql.memory.debug", "true")
      .toBoolean
  }
  val ACCUMULATED_LEAK_BYTES = new AtomicLong(0L)

  // For testing purpose only
  private var fallbackRegistry: Option[TaskResourceRegistry] = None

  // For testing purpose only
  def setFallbackRegistry(r: TaskResourceRegistry): Unit = {
    fallbackRegistry = Some(r)
  }

  // For testing purpose only
  def unsetFallbackRegistry(): Unit = {
    fallbackRegistry.foreach(r => r.releaseAll())
    fallbackRegistry = None
  }

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
      return fallbackRegistry match {
        case Some(r) => r
        case _ =>
          throw new IllegalStateException("No fallback instance of TaskResourceRegistry found.")
      }
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

  def addRecycler(name: String, prio: Int)(f: => Unit): Unit = {
    addAnonymousResource(new TaskResource {
      override def release(): Unit = f

      override def priority(): Int = prio

      override def resourceName(): String = name
    })
  }

  def addRecycler(id: String, name: String, prio: Int)(f: => Unit): Unit = {
    addResource(
      id,
      new TaskResource {
        override def release(): Unit = f

        override def priority(): Int = prio

        override def resourceName(): String = name
      })
  }

  def addResource[T <: TaskResource](id: String, resource: T): T = {
    getTaskResourceRegistry().addResource(id, resource)
  }

  def removeResource(id: String): Unit = {
    getTaskResourceRegistry().removeResource(id)
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

  def getSharedUsage(): SimpleMemoryUsageRecorder = {
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
            val currentTaskRegistries = RESOURCE_REGISTRIES.get(context)
            if (currentTaskRegistries == null) {
              throw new IllegalStateException(
                "TaskResourceRegistry is not initialized, this should not happen")
            }
            // We should first call `releaseAll` then remove the registries, because
            // the functions inside registries may register new resource to registries.
            currentTaskRegistries.releaseAll()
            context.taskMetrics().incPeakExecutionMemory(registry.getSharedUsage().peak())
            RESOURCE_REGISTRIES.remove(context)
          }
        }
      })
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
  private val sharedUsage = new SimpleMemoryUsageRecorder()
  type TaskResourceWithOrdering = (TaskResource, Int)
  private val resources = new util.HashMap[String, TaskResourceWithOrdering]()
  // A monotonically increasing accumulator to specify the task resource ordering
  // when inserting into queue
  private var resourceOrdering = 0
  // 0 is lowest, Int.MaxValue is highest
  private val resourcesPriorityQueue =
    new PriorityQueue[TaskResourceWithOrdering](new Comparator[TaskResourceWithOrdering]() {
      override def compare(t1: TaskResourceWithOrdering, t2: TaskResourceWithOrdering): Int = {
        val diff = t2._1.priority() - t1._1.priority()
        if (diff == 0) {
          // If the task resource has same priority, we should follow the LIFO
          t2._2 - t1._2
        } else {
          diff
        }
      }
    })

  private def addResource0(id: String, resource: TaskResource): Unit = synchronized {
    val resourceWithOrdering = (resource, resourceOrdering)
    resources.put(id, resourceWithOrdering)
    resourcesPriorityQueue.add(resourceWithOrdering)
    resourceOrdering += 1
  }

  /** Release all managed resources according to priority and reversed order */
  private[util] def releaseAll(): Unit = synchronized {
    while (!resourcesPriorityQueue.isEmpty) {
      val resource = resourcesPriorityQueue.poll()._1
      try {
        resource.release()
      } catch {
        case e: Throwable =>
          logWarning(s"Failed to call release() on task resource ${resource.resourceName()}", e)
      }
    }
    resources.clear()
    resourceOrdering = 0
  }

  private[util] def addResourceIfNotRegistered[T <: TaskResource](id: String, factory: () => T): T =
    synchronized {
      if (resources.containsKey(id)) {
        return resources.get(id)._1.asInstanceOf[T]
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
    resources.get(id)._1.asInstanceOf[T]
  }

  private[util] def getSharedUsage(): SimpleMemoryUsageRecorder = synchronized {
    sharedUsage
  }

  private[util] def removeResource(id: String): Unit = synchronized {
    if (!resources.containsKey(id)) {
      throw new IllegalArgumentException(
        String.format("TaskResource with ID %s is not registered", id))
    }
    resourcesPriorityQueue.remove(resources.get(id))
    resources.remove(id)
  }
}
