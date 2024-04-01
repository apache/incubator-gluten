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

import org.apache.spark.{TaskContext, TaskFailedReason, TaskKilledException, UnknownReason}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

import _root_.org.apache.gluten.memory.SimpleMemoryUsageRecorder
import _root_.org.apache.gluten.sql.shims.SparkShimLoader
import _root_.org.apache.gluten.utils.TaskListener

import java.util
import java.util.{Collections, UUID}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.compat.Platform.ConcurrentModificationException

object TaskResources extends TaskListener with Logging {
  // And open java assert mode to get memory stack
  val DEBUG: Boolean = {
    SQLConf.get
      .getConfString("spark.gluten.sql.memory.debug", "true")
      .toBoolean
  }
  val ACCUMULATED_LEAK_BYTES = new AtomicLong(0L)

  private def newUnsafeTaskContext(): TaskContext = {
    SparkShimLoader.getSparkShims.createTestTaskContext()
  }

  private def setUnsafeTaskContext(): Unit = {
    if (inSparkTask()) {
      throw new UnsupportedOperationException(
        "TaskResources#runUnsafe should only be used outside Spark task")
    }
    TaskContext.setTaskContext(newUnsafeTaskContext())
  }

  private def unsetUnsafeTaskContext(): Unit = {
    if (!inSparkTask()) {
      throw new IllegalStateException()
    }
    if (getLocalTaskContext().taskAttemptId() != -1) {
      throw new IllegalStateException()
    }
    TaskContext.unset()
  }

  // Run code with unsafe task context. If the call took place from Spark driver or test code
  // without a Spark task context registered, a temporary unsafe task context instance will
  // be created and used. Since unsafe task context is not managed by Spark's task memory manager,
  // Spark may not be aware of the allocations happened inside the user code.
  //
  // The API should only be used in the following cases:
  //
  // 1. Run code on driver
  // 2. Run test code
  def runUnsafe[T](body: => T): T = {
    TaskResources.setUnsafeTaskContext()
    onTaskStart()
    val context = getLocalTaskContext()
    try {
      val out =
        try {
          body
        } catch {
          case t: Throwable =>
            // Similar code with those in Task.scala
            try {
              context.markTaskFailed(t)
            } catch {
              case t: Throwable =>
                t.addSuppressed(t)
            }
            context.markTaskCompleted(Some(t))
            throw t
        } finally {
          try {
            context.markTaskCompleted(None)
          } finally {
            TaskResources.unsetUnsafeTaskContext()
          }
        }
      onTaskSucceeded()
      out
    } catch {
      case t: Throwable =>
        onTaskFailed(UnknownReason)
        throw t
    }
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
      throw new UnsupportedOperationException(
        "Not in a Spark task. If the code is running on driver or for testing purpose, " +
          "try using TaskResources#runUnsafe")
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

  def addResource[T <: TaskResource](id: String, resource: T): T = {
    getTaskResourceRegistry().addResource(id, resource)
  }

  def releaseResource(id: String): Unit = {
    getTaskResourceRegistry().releaseResource(id)
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
  private val resources = new util.HashMap[String, TaskResource]()
  private val priorityToResourcesMapping: util.HashMap[Int, util.LinkedHashSet[TaskResource]] =
    new util.HashMap[Int, util.LinkedHashSet[TaskResource]]()

  private var exclusiveLockAcquired: Boolean = false
  private def lock[T](body: => T): T = {
    synchronized {
      if (exclusiveLockAcquired) {
        throw new ConcurrentModificationException
      }
      body
    }
  }
  private def exclusiveLock[T](body: => T): T = {
    synchronized {
      if (exclusiveLockAcquired) {
        throw new ConcurrentModificationException
      }
      exclusiveLockAcquired = true
      try {
        body
      } finally {
        exclusiveLockAcquired = false
      }
    }
  }

  private def addResource0(id: String, resource: TaskResource): Unit = lock {
    resources.put(id, resource)
    priorityToResourcesMapping
      .computeIfAbsent(resource.priority(), _ => new util.LinkedHashSet[TaskResource]())
      .add(resource)
  }

  private def release(resource: TaskResource): Unit = exclusiveLock {
    // We disallow modification on registry's members when calling the user-defined release code.
    resource.release()
  }

  /** Release all managed resources according to priority and reversed order */
  private[util] def releaseAll(): Unit = lock {
    val table = new util.ArrayList(priorityToResourcesMapping.entrySet())
    Collections.sort(
      table,
      (
          o1: util.Map.Entry[Int, util.LinkedHashSet[TaskResource]],
          o2: util.Map.Entry[Int, util.LinkedHashSet[TaskResource]]) => {
        val diff = o2.getKey - o1.getKey // descending by priority
        if (diff > 0) 1
        else if (diff < 0) -1
        else throw new IllegalStateException("Unreachable code")
      }
    )
    table.forEach {
      _.getValue.asScala.toSeq.reverse
        .foreach(release(_)) // lifo for all resources within the same priority
    }
    priorityToResourcesMapping.clear()
    resources.clear()
  }

  /** Release single resource by ID */
  private[util] def releaseResource(id: String): Unit = lock {
    if (!resources.containsKey(id)) {
      throw new IllegalArgumentException(
        String.format("TaskResource with ID %s is not registered", id))
    }
    val resource = resources.get(id)
    if (!priorityToResourcesMapping.containsKey(resource.priority())) {
      throw new IllegalStateException("TaskResource's priority not found in priority mapping")
    }
    val samePrio = priorityToResourcesMapping.get(resource.priority())
    if (!samePrio.contains(resource)) {
      throw new IllegalStateException("TaskResource not found in priority mapping")
    }
    release(resource)
    samePrio.remove(resource)
    resources.remove(id)
  }

  private[util] def addResourceIfNotRegistered[T <: TaskResource](id: String, factory: () => T): T =
    lock {
      if (resources.containsKey(id)) {
        return resources.get(id).asInstanceOf[T]
      }
      val resource = factory.apply()
      addResource0(id, resource)
      resource
    }

  private[util] def addResource[T <: TaskResource](id: String, resource: T): T = lock {
    if (resources.containsKey(id)) {
      throw new IllegalArgumentException(
        String.format("TaskResource with ID %s is already registered", id))
    }
    addResource0(id, resource)
    resource
  }

  private[util] def isResourceRegistered(id: String): Boolean = lock {
    resources.containsKey(id)
  }

  private[util] def getResource[T <: TaskResource](id: String): T = lock {
    if (!resources.containsKey(id)) {
      throw new IllegalArgumentException(
        String.format("TaskResource with ID %s is not registered", id))
    }
    resources.get(id).asInstanceOf[T]
  }

  private[util] def getSharedUsage(): SimpleMemoryUsageRecorder = lock {
    sharedUsage
  }
}
