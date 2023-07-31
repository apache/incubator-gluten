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

import org.apache.spark.internal.Logging

import _root_.io.glutenproject.memory.TaskMemoryMetrics

import java.util
import java.util.Collections

import scala.collection.JavaConverters._

object GlutenTaskContext {

  def get(): TaskResourceRegistry = registry.get

  private[this] val registry: ThreadLocal[TaskResourceRegistry] =
    new ThreadLocal[TaskResourceRegistry]

  protected[spark] def setTaskContext(tc: TaskResourceRegistry): Unit = registry.set(tc)

  protected[spark] def unset(): Unit = registry.remove()
}

class TaskResourceRegistry extends Logging {
  private val sharedMetrics = new TaskMemoryMetrics()
  private val resources = new java.util.LinkedHashMap[String, TaskResource]()
  private val resourcesPriorityMapping =
    new java.util.HashMap[Long, java.util.List[TaskResource]]()

  private def addResource0(id: String, resource: TaskResource): Unit = {
    resources.put(id, resource)
    if (!resourcesPriorityMapping.containsKey(resource.priority())) {
      resourcesPriorityMapping.put(resource.priority(), new util.ArrayList[TaskResource]())
    }
    val list = resourcesPriorityMapping.get(resource.priority())
    list.add(resource)
    logInfo(s"Register TaskResource ${resource.resourceName()}")
  }

  private[util] def releaseAll(): Unit = {
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
              m.release()
              logInfo(s"Release TaskResource ${m.resourceName()}")
            } catch {
              case e: Throwable =>
                logWarning("Failed to call release() on resource instance", e)
            })
    }
  }

  private[util] def addResourceIfNotRegistered[T <: TaskResource](
      id: String,
      factory: () => T): T = {
    if (resources.containsKey(id)) {
      return resources.get(id).asInstanceOf[T]
    }
    val resource = factory.apply()
    addResource0(id, resource)
    resource
  }

  private[util] def addResource[T <: TaskResource](id: String, resource: T): T = {
    if (resources.containsKey(id)) {
      throw new IllegalArgumentException(
        String.format("TaskResource with ID %s is already registered", id))
    }
    addResource0(id, resource)
    resource
  }

  private[util] def isResourceRegistered(id: String): Boolean = {
    resources.containsKey(id)
  }

  private[util] def getResource[T <: TaskResource](id: String): T = {
    if (!resources.containsKey(id)) {
      throw new IllegalArgumentException(
        String.format("TaskResource with ID %s is not registered", id))
    }
    resources.get(id).asInstanceOf[T]
  }

  private[util] def getSharedMetrics(): TaskMemoryMetrics = {
    sharedMetrics
  }
}
