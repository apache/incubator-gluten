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

import io.glutenproject.backendsapi.BackendsApiManager;
import org.apache.spark.util.memory.TaskResources;

/**
 * Built-in toolkit for managing native memory allocations. To use the facility, one should
 * import Gluten's C++ library then create the c++ instance using following example code:
 * <p>
 * ```c++
 * auto* allocator = reinterpret_cast<gluten::memory::MemoryAllocator*>(allocator_id);
 * ```
 * <p>
 * The ID "allocator_id" can be retrieved from Java API
 * {@link NativeMemoryAllocator#getNativeInstanceId()}.
 * <p>
 * FIXME: to export the native APIs in a standard way
 */
public abstract class NativeMemoryAllocators {
  private NativeMemoryAllocators() {
  }

  private static final NativeMemoryAllocator GLOBAL = NativeMemoryAllocator.getDefault();

  public static NativeMemoryAllocator contextInstance() {
    if (!TaskResources.inSparkTask()) {
      return globalInstance();
    }

    final String id = NativeMemoryAllocatorManager.class.toString();
    if (!TaskResources.isResourceManagerRegistered(id)) {
      final NativeMemoryAllocatorManager manager = BackendsApiManager.getIteratorApiInstance()
          .genNativeMemoryAllocatorManager(
              TaskResources.getSparkMemoryManager(),
              Spiller.NO_OP,
              TaskResources.getSharedMetrics());
      TaskResources.addResourceManager(id, manager);
    }
    return ((NativeMemoryAllocatorManager) TaskResources.getResourceManager(id)).getManaged();
  }

  public static NativeMemoryAllocator createSpillable(Spiller spiller) {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException("Spiller must be used in a Spark task");
    }

    final NativeMemoryAllocatorManager manager = BackendsApiManager.getIteratorApiInstance()
        .genNativeMemoryAllocatorManager(
            TaskResources.getSparkMemoryManager(),
            spiller,
            TaskResources.getSharedMetrics());
    TaskResources.addAnonymousResourceManager(manager);
    return manager.getManaged();
  }

  public static NativeMemoryAllocator globalInstance() {
    return GLOBAL;
  }
}
