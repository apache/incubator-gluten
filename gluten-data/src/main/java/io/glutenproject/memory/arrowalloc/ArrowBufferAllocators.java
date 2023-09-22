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
package io.glutenproject.memory.arrowalloc;

import io.glutenproject.memory.memtarget.MemoryTargets;
import io.glutenproject.memory.memtarget.spark.Spiller;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.spark.util.TaskResource;
import org.apache.spark.util.TaskResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Vector;

public class ArrowBufferAllocators {

  private ArrowBufferAllocators() {}

  private static final BufferAllocator GLOBAL = new RootAllocator(Long.MAX_VALUE);

  public static BufferAllocator globalInstance() {
    return GLOBAL;
  }

  public static BufferAllocator contextInstance() {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException("This method must be called in a Spark task.");
    }
    String id = ArrowBufferAllocatorManager.class.toString();
    return TaskResources.addResourceIfNotRegistered(id, ArrowBufferAllocatorManager::new).managed;
  }

  public static class ArrowBufferAllocatorManager implements TaskResource {
    private static Logger LOGGER = LoggerFactory.getLogger(ArrowBufferAllocatorManager.class);
    private static final List<BufferAllocator> LEAKED = new Vector<>();
    private final AllocationListener listener =
        new ManagedAllocationListener(
            MemoryTargets.throwOnOom(
                MemoryTargets.newConsumer(
                    TaskResources.getLocalTaskContext().taskMemoryManager(),
                    "ArrowContextInstance",
                    Spiller.NO_OP,
                    Collections.emptyMap())),
            TaskResources.getSharedUsage());
    private final BufferAllocator managed = new RootAllocator(listener, Long.MAX_VALUE);

    public ArrowBufferAllocatorManager() {}

    private void close() {
      managed.close();
    }

    private void softClose() {
      // move to leaked list
      long leakBytes = managed.getAllocatedMemory();
      long accumulated = TaskResources.ACCUMULATED_LEAK_BYTES().addAndGet(leakBytes);
      LOGGER.warn(
          String.format(
              "Detected leaked Arrow allocator, size: %d, "
                  + "process accumulated leaked size: %d...",
              leakBytes, accumulated));
      if (TaskResources.DEBUG()) {
        LOGGER.warn(String.format("Leaked allocator stack %s", managed.toVerboseString()));
        LEAKED.add(managed);
      }
    }

    @Override
    public void release() throws Exception {
      if (managed.getAllocatedMemory() != 0L) {
        softClose();
      } else {
        close();
      }
    }

    @Override
    public int priority() {
      return 0; // lowest priority
    }

    @Override
    public String resourceName() {
      return "ArrowBufferAllocatorManager";
    }
  }
}
