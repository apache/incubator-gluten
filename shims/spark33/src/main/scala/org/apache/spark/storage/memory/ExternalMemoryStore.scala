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
package org.apache.spark.storage.memory

import org.apache.gluten.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{MEMORY_OFFHEAP_SIZE, MEMORY_STORAGE_FRACTION, STORAGE_MEMORY_EVICT_PREFERENCE}
import org.apache.spark.memory.{MemoryManager, MemoryMode}
import org.apache.spark.storage.BlockId

private[spark] class ExternalMemoryStore(conf: SparkConf, memoryManager: MemoryManager)
  extends Logging {

  // Note: all changes to memory allocations, notably evicting entries and
  // acquiring memory, must be synchronized on `memoryManager`!

  private[spark] def initExternalCache(conf: SparkConf): Unit = {
    val reservationListener = new ExtMemStoreReservationListener(this)
    if (GlutenConfig.getConf.enableVeloxCache) {
      // scalastyle:off println
      println("*******Will initializing data cache late in velox backend")
      // scalastyle:on println
      val size = (conf.get(MEMORY_OFFHEAP_SIZE) * conf.get(MEMORY_STORAGE_FRACTION) * 1.1).toLong
      GlutenMemStoreInjects.setMemStoreSize(size)
      GlutenMemStoreInjects.setReservationListener(reservationListener)
    }
  }

  // This method will call memoryManager.acquireStorageMemory()
  // NativeStorageMemoryListener will connect this method with velox memory allocator.allocate()
  private[spark] def acquireStorageMemory(numBytes: Long): Boolean = memoryManager.synchronized {
    // scalastyle:off println
    println("*******acquireStorageMemory in NativeMemoryStore")
    // scalastyle:on println
    memoryManager.acquireStorageMemory(BlockId("test_file_cache"), numBytes, MemoryMode.OFF_HEAP)
  }
  // We don't need handle increase spaceToFree in storage memory pool in this method
  // as releaseStorageMemory() will be automatically triggered during eviction
  private[spark] def evictEntriesToFreeSpace(spaceToFree: Long): Long = memoryManager.synchronized {
    if (GlutenConfig.getConf.enableVeloxCache) {
      // scalastyle:off println
      println("*******trying to evict data cache: " + spaceToFree)
      // scalastyle:on println
      GlutenMemStoreInjects.getInstance().evictEntriesToFreeSpace(spaceToFree)
    } else {
      0L
    }
  }
  // This method will call memoryManager.releaseStorageMemory()
  // NativeStorageMemoryListener will connect this method with velox memory allocator.free()
  private[spark] def releaseStorageMemory(numBytes: Long): Unit = memoryManager.synchronized {
    // scalastyle:off println
    println("*******releaseStorageMemory in NativeMemoryStore")
    // scalastyle:on println
    memoryManager.releaseStorageMemory(numBytes, MemoryMode.OFF_HEAP)
  }
}
