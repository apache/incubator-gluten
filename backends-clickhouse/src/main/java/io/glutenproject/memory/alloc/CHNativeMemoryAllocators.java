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

import io.glutenproject.memory.TaskMemoryMetrics;
import io.glutenproject.memory.memtarget.spark.GlutenMemoryConsumer;
import io.glutenproject.memory.memtarget.spark.Spiller;

import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.TaskResources;

/**
 * Built-in toolkit for managing native memory allocations. To use the facility, one should import
 * Gluten's C++ library then create the c++ instance using following example code:
 *
 * <p>```c++ auto* allocator = reinterpret_cast<gluten::memory::MemoryAllocator*>(allocator_id); ```
 *
 * <p>The ID "allocator_id" can be retrieved from Java API {@link
 * CHNativeMemoryAllocator#getNativeInstanceId()}.
 *
 * <p>FIXME: to export the native APIs in a standard way
 */
public abstract class CHNativeMemoryAllocators {
  private CHNativeMemoryAllocators() {}

  private static final CHNativeMemoryAllocator GLOBAL = CHNativeMemoryAllocator.getDefault();

  private static CHNativeMemoryAllocatorManager createNativeMemoryAllocatorManager(
      String name,
      TaskMemoryManager taskMemoryManager,
      Spiller spiller,
      TaskMemoryMetrics taskMemoryMetrics) {

    CHManagedCHReservationListener rl =
        new CHManagedCHReservationListener(
            new GlutenMemoryConsumer(name, taskMemoryManager, spiller), taskMemoryMetrics);
    return new CHNativeMemoryAllocatorManagerImpl(CHNativeMemoryAllocator.createListenable(rl));
  }

  public static CHNativeMemoryAllocator contextInstance() {
    if (!TaskResources.inSparkTask()) {
      return globalInstance();
    }

    final String id = CHNativeMemoryAllocatorManager.class.toString();
    if (!TaskResources.isResourceRegistered(id)) {
      final CHNativeMemoryAllocatorManager manager =
          createNativeMemoryAllocatorManager(
              "ContextInstance",
              TaskResources.getLocalTaskContext().taskMemoryManager(),
              Spiller.NO_OP,
              TaskResources.getSharedMetrics());
      TaskResources.addResource(id, manager);
    }
    return ((CHNativeMemoryAllocatorManager) TaskResources.getResource(id)).getManaged();
  }

  public static CHNativeMemoryAllocator contextInstanceForUT() {
    return CHNativeMemoryAllocator.getDefaultForUT();
  }

  public static CHNativeMemoryAllocator createSpillable(String name, Spiller spiller) {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException("spiller must be used in a Spark task");
    }

    final CHNativeMemoryAllocatorManager manager =
        createNativeMemoryAllocatorManager(
            name,
            TaskResources.getLocalTaskContext().taskMemoryManager(),
            spiller,
            TaskResources.getSharedMetrics());
    TaskResources.addAnonymousResource(manager);
    return manager.getManaged();
  }

  public static CHNativeMemoryAllocator globalInstance() {
    return GLOBAL;
  }
}
