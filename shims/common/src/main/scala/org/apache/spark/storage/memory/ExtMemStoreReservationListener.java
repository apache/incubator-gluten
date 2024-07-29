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
package org.apache.spark.storage.memory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reserve Spark managed memory. */
public class ExtMemStoreReservationListener {

  private static final Logger LOG =
      LoggerFactory.getLogger(ExtMemStoreReservationListener.class);

  private final ExternalMemoryStore extMemoryStore;

  public ExtMemStoreReservationListener(ExternalMemoryStore extMemoryStore) {
    this.extMemoryStore = extMemoryStore;
  }

  public long reserve(long size) {
    synchronized (this) {
      try {
        System.out.println("*******Data cache tries to acquire " + size + " bytes memory.");
        // throw new RuntimeException("Failed acquire enough memory from storage memory pool");
        boolean success = extMemoryStore.acquireStorageMemory(size);
        if (success) {
          return size;
        } else {
          throw new RuntimeException("Failed acquire enough memory from storage memory pool");
        }
      } catch (Exception e) {
        LOG.error("Error reserving memory from native memory store", e);
        throw e;
      }
    }
  }

  public long unreserve(long size) {
    synchronized (this) {
      System.out.println("*******Data cache tries to release " + size + " bytes memory.");
      extMemoryStore.releaseStorageMemory(size);
      return size;
    }
  }
}
