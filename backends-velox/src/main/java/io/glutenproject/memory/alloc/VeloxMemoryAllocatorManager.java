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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.util.memory.TaskMemoryResourceManager;
import org.apache.spark.util.memory.TaskMemoryResources;

public class VeloxMemoryAllocatorManager implements TaskMemoryResourceManager,
    NativeMemoryAllocatorManager {

  private static Logger LOGGER = LoggerFactory.getLogger(VeloxMemoryAllocatorManager.class);
  private static final List<NativeMemoryAllocator> LEAKED = new Vector<>();
  private final NativeMemoryAllocator managed;

  public VeloxMemoryAllocatorManager(NativeMemoryAllocator managed) {
    this.managed = managed;
  }

  private void close() throws Exception {
    managed.close();
  }

  private void softClose() throws Exception {
    // move to leaked list
    long leakBytes = managed.getBytesAllocated();
    long accumulated = TaskMemoryResources.ACCUMULATED_LEAK_BYTES().addAndGet(leakBytes);
    LOGGER.warn(String.format("Detected leaked native allocator, size: %d, " +
        "process accumulated leaked size: %d...", leakBytes, accumulated));
    managed.listener().inactivate();
    if (TaskMemoryResources.DEBUG()) {
      LEAKED.add(managed);
    }
  }

  @Override
  public void release() throws Exception {
    if (managed.getBytesAllocated() != 0L) {
      softClose();
    } else {
      close();
    }
  }

  @Override
  public NativeMemoryAllocator getManaged() {
    return managed;
  }
}
