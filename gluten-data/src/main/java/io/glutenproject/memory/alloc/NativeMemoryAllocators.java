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
import io.glutenproject.memory.memtarget.MemoryTarget;
import io.glutenproject.memory.memtarget.OverAcquire;
import io.glutenproject.memory.memtarget.spark.GlutenMemoryConsumer;
import io.glutenproject.memory.memtarget.spark.Spiller;

import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.TaskResources;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Built-in toolkit for managing native memory allocations. To use the facility, one should import
 * Gluten's C++ library then create the c++ instance using following example code:
 *
 * <p>```c++ auto* allocator = reinterpret_cast<gluten::memory::MemoryAllocator*>(allocator_id); ```
 *
 * <p>The ID "allocator_id" can be retrieved from Java API {@link
 * NativeMemoryAllocator#getNativeInstanceId()}.
 *
 * <p>FIXME: to export the native APIs in a standard way
 */
public final class NativeMemoryAllocators {
  private static final Map<NativeMemoryAllocator.Type, NativeMemoryAllocators> INSTANCES =
      new ConcurrentHashMap<>();

  private final NativeMemoryAllocator global;

  private NativeMemoryAllocators(NativeMemoryAllocator.Type type) {
    global = NativeMemoryAllocator.create(type);
  }

  public static NativeMemoryAllocators getDefault() {
    return forType(NativeMemoryAllocator.Type.DEFAULT);
  }

  private static NativeMemoryAllocators forType(NativeMemoryAllocator.Type type) {
    return INSTANCES.computeIfAbsent(type, NativeMemoryAllocators::new);
  }

  // TODO: When remove fallback storage and ensure all allocator are in Spark task scope,
  //  we could remove this method and use create.
  public NativeMemoryAllocator contextInstance(String name) {
    if (!TaskResources.inSparkTask()) {
      return globalInstance();
    }
    final String id = NativeMemoryAllocatorManager.class + "-"
        + System.identityHashCode(global) + "-" + name;
    return TaskResources.addResourceIfNotRegistered(
            id,
            () ->
                createNativeMemoryAllocatorManager(
                    name,
                    TaskResources.getLocalTaskContext().taskMemoryManager(),
                    TaskResources.getSharedMetrics(),
                    0.0D,
                    Spiller.NO_OP,
                    global))
        .getManaged();
  }

  public NativeMemoryAllocator create(String name, double overAcquiredRatio, Spiller spiller) {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException("Spiller must be used in a Spark task");
    }

    final NativeMemoryAllocatorManager manager =
        createNativeMemoryAllocatorManager(
            name,
            TaskResources.getLocalTaskContext().taskMemoryManager(),
            TaskResources.getSharedMetrics(),
            overAcquiredRatio,
            spiller,
            global);
    return TaskResources.addAnonymousResource(manager).getManaged();
  }

  public NativeMemoryAllocator globalInstance() {
    return global;
  }

  private static NativeMemoryAllocatorManager createNativeMemoryAllocatorManager(
      String name,
      TaskMemoryManager taskMemoryManager,
      TaskMemoryMetrics taskMemoryMetrics,
      double overAcquiredRatio,
      Spiller spiller,
      NativeMemoryAllocator delegated) {
    MemoryTarget target = new GlutenMemoryConsumer(name, taskMemoryManager, spiller);
    if (overAcquiredRatio != 0.0D) {
      target =
          new OverAcquire(
              target, new OverAcquire.DummyTarget(taskMemoryManager), overAcquiredRatio);
    }
    ManagedReservationListener rl = new ManagedReservationListener(target, taskMemoryMetrics);
    return new NativeMemoryAllocatorManagerImpl(
        NativeMemoryAllocator.createListenable(rl, delegated));
  }
}
