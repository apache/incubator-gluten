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

package io.glutenproject.memory.alloc;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.util.memory.TaskMemoryResourceManager;

public class CHMemoryAllocatorManager implements TaskMemoryResourceManager,
    NativeMemoryAllocatorManager {

  private static Logger LOGGER = LoggerFactory.getLogger(CHMemoryAllocatorManager.class);

  private static final List<NativeMemoryAllocator> LEAKED = new Vector<>();
  private final NativeMemoryAllocator managed;

  private static AtomicLong ACCUMULATED_LEAK_BYTES = new AtomicLong(0L);

  public CHMemoryAllocatorManager(NativeMemoryAllocator managed) {
    this.managed = managed;
  }

  private void close() throws Exception {
    managed.close();
  }

  @Override
  public void release() throws Exception {
    // close native allocator first
    close();
    // detect whether there is the memory leak
    long currentMemory = managed.listener().currentMemory();
    managed.listener().inactivate();
    if (currentMemory > 0L) {
      long accumulated = ACCUMULATED_LEAK_BYTES.addAndGet(currentMemory);
      String errMsg = String.format("Detected leaked native allocator, size: %d, " +
          "process accumulated leaked size: %d...", currentMemory, accumulated);
      LOGGER.error(errMsg);
      throw new UnsupportedOperationException(errMsg);
    }
  }

  @Override
  public NativeMemoryAllocator getManaged() {
    return managed;
  }
}
