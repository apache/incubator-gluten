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
import org.apache.gluten.memory.{MemoryUsageRecorder, SimpleMemoryUsageRecorder}
import org.apache.gluten.memory.listener.ReservationListener

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.BlockId

import java.lang.reflect.Field
import java.util.UUID

/**
 * API #acuqire is for reserving some global off-heap memory from Spark memory manager. Once
 * reserved, Spark tasks will have less off-heap memory to use because of the reservation.
 *
 * Note the API #acuqire doesn't trigger spills on Spark tasks although OOM may be encountered.
 *
 * The utility internally relies on the Spark storage memory pool. As Spark doesn't expect trait
 * BlockId to be extended by user, TestBlockId is chosen for the storage memory reservations.
 */
object GlobalOffHeapMemory extends Logging {
  private val recorder: MemoryUsageRecorder = new SimpleMemoryUsageRecorder()

  private val FIELD_MEMORY_MANAGER: Field = {
    val f =
      try {
        classOf[TaskMemoryManager].getDeclaredField("memoryManager")
      } catch {
        case e: Exception =>
          throw new GlutenException(
            "Unable to find field TaskMemoryManager#memoryManager via reflection",
            e)
      }
    f.setAccessible(true)
    f
  }

  def acquire(numBytes: Long): Unit = memoryManagerOption().foreach {
    mm =>
      val succeeded =
        mm.acquireStorageMemory(
          BlockId(s"test_${UUID.randomUUID()}"),
          numBytes,
          MemoryMode.OFF_HEAP)

      if (succeeded) {
        recorder.inc(numBytes)
        return
      }

      // Throw OOM.
      val offHeapMemoryTotal =
        mm.maxOffHeapStorageMemory + mm.offHeapExecutionMemoryUsed
      throw new GlutenException(
        s"Spark off-heap memory is exhausted." +
          s" Storage: ${mm.offHeapStorageMemoryUsed} / $offHeapMemoryTotal," +
          s" execution: ${mm.offHeapExecutionMemoryUsed} / $offHeapMemoryTotal")
  }

  def release(numBytes: Long): Unit = memoryManagerOption().foreach {
    mm =>
      mm.releaseStorageMemory(numBytes, MemoryMode.OFF_HEAP)
      recorder.inc(-numBytes)
  }

  def currentBytes(): Long = {
    recorder.current()
  }

  def newReservationListener(): ReservationListener = {
    new ReservationListener {
      private val recorder: MemoryUsageRecorder = new SimpleMemoryUsageRecorder()

      override def reserve(size: Long): Long = {
        acquire(size)
        recorder.inc(size)
        size
      }

      override def unreserve(size: Long): Long = {
        release(size)
        recorder.inc(-size)
        size
      }

      override def getUsedBytes: Long = {
        recorder.current()
      }
    }
  }

  private def memoryManagerOption(): Option[MemoryManager] = {
    val env = SparkEnv.get
    if (env != null) {
      return Some(env.memoryManager)
    }
    val tc = TaskContext.get()
    if (tc != null) {
      // This may happen in test code that mocks the task context without booting up SparkEnv.
      return Some(FIELD_MEMORY_MANAGER.get(tc.taskMemoryManager()).asInstanceOf[MemoryManager])
    }
    logWarning(
      "Memory manager not found because the code is unlikely be run in a Spark application")
    None
  }
}
