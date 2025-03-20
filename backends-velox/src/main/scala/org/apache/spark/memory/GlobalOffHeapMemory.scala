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
package org.apache.spark.memory

import org.apache.gluten.exception.GlutenException

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.storage.BlockId

import java.lang.reflect.Field
import java.util.UUID

object GlobalOffHeapMemory {
  private val FIELD_MEMORY_MANAGER: Field = {
    val f = try {
      classOf[TaskMemoryManager].getDeclaredField("memoryManager")
    } catch {
      case e: Exception =>
        throw new GlutenException("Unable to find field TaskMemoryManager#memoryManager via reflection", e)
    }
    f.setAccessible(true)
    f
  }

  def acquire(numBytes: Long): Boolean = {
    memoryManager().acquireStorageMemory(
      BlockId(s"test_${UUID.randomUUID()}"),
      numBytes,
      MemoryMode.OFF_HEAP)
  }

  def free(numBytes: Long): Unit = {
    memoryManager().releaseStorageMemory(numBytes, MemoryMode.OFF_HEAP)
  }

  private def memoryManager(): MemoryManager = {
    val env = SparkEnv.get
    if (env != null) {
      return env.memoryManager
    }
    val tc = TaskContext.get()
    if (tc != null) {
      // This may happen in test code that mocks the task context without booting up SparkEnv.
      return FIELD_MEMORY_MANAGER.get(tc.taskMemoryManager()).asInstanceOf[MemoryManager]
    }
    throw new GlutenException(
      "Memory manager not found because the code is unlikely be run in a Spark application")
  }
}
