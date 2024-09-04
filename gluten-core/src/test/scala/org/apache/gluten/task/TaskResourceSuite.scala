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
package org.apache.gluten.task

import org.apache.spark.memory.{MemoryConsumer, MemoryMode}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.task.{SparkTaskUtil, TaskResource, TaskResources}

import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID

class TaskResourceSuite extends AnyFunSuite with SQLHelper {
  test("Run unsafe") {
    val out = TaskResources.runUnsafe {
      1
    }
    assert(out == 1)
  }

  test("Run unsafe - task context") {
    TaskResources.runUnsafe {
      assert(TaskResources.inSparkTask())
      assert(TaskResources.getLocalTaskContext() != null)
    }
  }

  test("Run unsafe - propagate Spark config") {
    val total = 128 * 1024 * 1024
    withSQLConf(
      "spark.memory.offHeap.enabled" -> "true",
      "spark.memory.offHeap.size" -> s"$total",
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      TaskResources.runUnsafe {
        assert(TaskResources.inSparkTask())
        assert(TaskResources.getLocalTaskContext() != null)

        val tmm = SparkTaskUtil.getTaskMemoryManager(TaskResources.getLocalTaskContext())
        val consumer = new MemoryConsumer(tmm, MemoryMode.OFF_HEAP) {
          override def spill(size: Long, trigger: MemoryConsumer): Long = 0L
        }
        assert(consumer.acquireMemory(total) == total)
        assert(consumer.acquireMemory(1) == 0)

        assert(!SQLConf.get.adaptiveExecutionEnabled)
      }
    }
  }

  test("Run unsafe - register resource") {
    var unregisteredCount = 0
    TaskResources.runUnsafe {
      TaskResources.addResource(
        UUID.randomUUID().toString,
        new TaskResource {
          override def release(): Unit = unregisteredCount += 1

          override def resourceName(): String = "test resource 1"
        })
      TaskResources.addResource(
        UUID.randomUUID().toString,
        new TaskResource {
          override def release(): Unit = unregisteredCount += 1

          override def resourceName(): String = "test resource 2"
        })
    }
    assert(unregisteredCount == 2)
  }
}
