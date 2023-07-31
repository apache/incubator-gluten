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
import _root_.io.glutenproject.memory.TaskMemoryMetrics
import _root_.io.glutenproject.utils.TaskListener

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

object TaskResources extends TaskListener with Logging {
  // And open java assert mode to get memory stack
  val DEBUG: Boolean = {
    SQLConf.get
      .getConfString("spark.gluten.sql.memory.debug", "true")
      .toBoolean
  }
  val ACCUMULATED_LEAK_BYTES = new AtomicLong(0L)

  def getLocalTaskContext(): TaskContext = {
    TaskContext.get()
  }

  def inSparkTask(): Boolean = {
    TaskContext.get() != null
  }

  private def getTaskResourceRegistry(): TaskResourceRegistry = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Not in a Spark task")
    }
    GlutenTaskContext.get()
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

  def getSharedMetrics(): TaskMemoryMetrics = {
    getTaskResourceRegistry().getSharedMetrics()
  }

  override def onTaskStart(): Unit = {
    if (!inSparkTask()) {
      throw new IllegalStateException("Not in a Spark task")
    }
    val tc = getLocalTaskContext()
    val taskResourceRegistry = new TaskResourceRegistry
    GlutenTaskContext.setTaskContext(taskResourceRegistry)

    tc.addTaskFailureListener(
      // in case of crashing in task completion listener, errors may be swallowed
      new TaskFailureListener {
        override def onTaskFailure(context: TaskContext, error: Throwable): Unit = {
          GlutenTaskContext.unset()
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
        val registry = getTaskResourceRegistry()
        registry.releaseAll()
        context.taskMetrics().incPeakExecutionMemory(registry.getSharedMetrics().peak())
        GlutenTaskContext.unset()
      }
    })

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
