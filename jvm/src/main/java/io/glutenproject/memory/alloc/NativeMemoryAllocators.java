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

import org.apache.spark.util.memory.TaskMemoryResourceManager;
import org.apache.spark.util.memory.TaskMemoryResources;

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
      final TaskMemoryResourceManager manager = BackendsApiManager.getIteratorApiInstance()
          .genNativeMemoryAllocatorManager(
              TaskMemoryResources.getSparkMemoryManager(),
              Spiller.NO_OP,
              TaskMemoryResources.getSharedMetrics());
      TaskMemoryResources.addResourceManager(id, manager);
    }
    return TaskMemoryResources.getResourceManager(id).getManaged();
  }

  public static NativeMemoryAllocator createSpillable(Spiller spiller) {
    if (!TaskMemoryResources.inSparkTask()) {
      throw new IllegalStateException("Spiller must be used in a Spark task");
    }

    final TaskMemoryResourceManager manager = BackendsApiManager.getIteratorApiInstance()
        .genNativeMemoryAllocatorManager(
            TaskMemoryResources.getSparkMemoryManager(),
            spiller,
            TaskMemoryResources.getSharedMetrics());
    TaskMemoryResources.addAnonymousResourceManager(manager);
    return manager.getManaged();
  }

  public static NativeMemoryAllocator globalInstance() {
    return GLOBAL;
  }
}
