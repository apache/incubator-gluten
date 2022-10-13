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

import io.glutenproject.memory.GlutenMemoryConsumer;
import org.apache.spark.internal.Logging;
import org.apache.spark.util.memory.TaskMemoryResourceManager;
import org.apache.spark.util.memory.TaskMemoryResources;

import java.util.List;
import java.util.Vector;

public class NativeMemoryAllocators {
  private NativeMemoryAllocators() {
  }

  private static final NativeMemoryAllocator GLOBAL = NativeMemoryAllocator.getDefault();

  public static NativeMemoryAllocator contextInstance() {
    if (!TaskMemoryResources.inSparkTask()) {
      return globalInstance();
    }

    final String id = NativeMemoryAllocatorManager.class.toString();
    if (!TaskMemoryResources.isResourceManagerRegistered(id)) {
      final ReservationListener rl = new SparkManagedReservationListener(
          new GlutenMemoryConsumer(TaskMemoryResources.getSparkMemoryManager(), Spiller.NO_OP),
          TaskMemoryResources.getSharedMetrics());
      final NativeMemoryAllocator alloc = NativeMemoryAllocator.createListenable(rl);
      final NativeMemoryAllocatorManager manager = new NativeMemoryAllocatorManager(alloc);
      TaskMemoryResources.addResourceManager(id, manager);
    }
    return ((NativeMemoryAllocatorManager) TaskMemoryResources.getResourceManager(id)).managed;
  }

  public static NativeMemoryAllocator createSpillable(Spiller spiller) {
    if (!TaskMemoryResources.inSparkTask()) {
      throw new IllegalStateException("Spiller must be used in a Spark task");
    }

    final ReservationListener rl = new SparkManagedReservationListener(
        new GlutenMemoryConsumer(TaskMemoryResources.getSparkMemoryManager(), spiller),
        TaskMemoryResources.getSharedMetrics());
    final NativeMemoryAllocator alloc = NativeMemoryAllocator.createListenable(rl);
    final NativeMemoryAllocatorManager manager = new NativeMemoryAllocatorManager(alloc);
    TaskMemoryResources.addAnonymousResourceManager(manager);
    return manager.managed;
  }

  public static NativeMemoryAllocator globalInstance() {
    return GLOBAL;
  }

  public static class NativeMemoryAllocatorManager implements TaskMemoryResourceManager, Logging {
    private static final List<NativeMemoryAllocator> LEAKED = new Vector<>();
    private final NativeMemoryAllocator managed;

    public NativeMemoryAllocatorManager(NativeMemoryAllocator managed) {
      this.managed = managed;
    }

    private void close() throws Exception {
      managed.close();
    }

    private void softClose() throws Exception {
      // move to leaked list
      long leakBytes = managed.getBytesAllocated();
      long accumulated = TaskMemoryResources.ACCUMULATED_LEAK_BYTES().addAndGet(leakBytes);
      logWarning(() -> String.format("Detected leaked native allocator, size: %d, " +
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
  }
}
