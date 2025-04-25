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

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.exception.GlutenException
import org.apache.gluten.memory.memtarget.{MemoryTarget, NoopMemoryTarget}

/**
 * API #acuqire is for reserving some global off-heap memory from Spark memory manager. Once
 * reserved, Spark tasks will have less off-heap memory to use because of the reservation.
 *
 * Note the API #acuqire doesn't trigger spills on Spark tasks although OOM may be encountered.
 *
 * The utility internally relies on the Spark storage memory pool. As Spark doesn't expect trait
 * BlockId to be extended by user, TestBlockId is chosen for the storage memory reservations.
 */
object GlobalOffHeapMemory {
  private val target: MemoryTarget = if (GlutenConfig.get.memoryUntracked) {
    new NoopMemoryTarget()
  } else {
    new GlobalOffHeapMemoryTarget()
  }

  def acquire(numBytes: Long): Unit = {
    if (target.borrow(numBytes) < numBytes) {
      // Throw OOM.
      throw new GlutenException(s"Spark global off-heap memory is exhausted.")
    }
  }

  def release(numBytes: Long): Unit = {
    assert(target.repay(numBytes) == numBytes)
  }

  def currentBytes(): Long = {
    target.usedBytes()
  }
}
