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
package io.glutenproject.memory.nmm;

import io.glutenproject.GlutenConfig;
import io.glutenproject.memory.TaskMemoryMetrics;
import io.glutenproject.memory.memtarget.MemoryTarget;
import io.glutenproject.memory.memtarget.OverAcquire;
import io.glutenproject.memory.memtarget.spark.GlutenMemoryConsumer;
import io.glutenproject.memory.memtarget.spark.Spiller;

import org.apache.spark.TaskContext;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.TaskResources;

public final class NativeMemoryManagers {

  // TODO: Let all caller support spill.
  public static NativeMemoryManager contextInstance(String name) {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException("This method must be called in a Spark task.");
    }
    return TaskResources.addResourceIfNotRegistered(
        name,
        () ->
            createNativeMemoryManager(
                name,
                createMemoryTarget(name, TaskContext.get().taskMemoryManager(), Spiller.NO_OP),
                TaskResources.getSharedMetrics()));
  }

  /** Create a temporary memory manager, caller should call NativeMemoryManager#release manually. */
  public static NativeMemoryManager tmpInstance(String name) {
    if (TaskResources.inSparkTask()) {
      throw new IllegalStateException("This method should not used here.");
    }
    return NativeMemoryManager.create(name, ReservationListener.NOOP);
  }

  public static NativeMemoryManager create(String name, Spiller spiller) {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException("Spiller must be used in a Spark task.");
    }

    final NativeMemoryManager manager =
        createNativeMemoryManager(
            name,
            createMemoryTarget(name, TaskContext.get().taskMemoryManager(), spiller),
            TaskResources.getSharedMetrics());
    return TaskResources.addAnonymousResource(manager);
  }

  public static MemoryTarget createMemoryTarget(
      String name, TaskMemoryManager taskMemoryManager, Spiller spiller) {
    double overAcquiredRatio = GlutenConfig.getConf().veloxOverAcquiredMemoryRatio();
    MemoryTarget target = new GlutenMemoryConsumer(name, taskMemoryManager, spiller);
    if (overAcquiredRatio != 0.0D) {
      target =
          new OverAcquire(
              target, new OverAcquire.DummyTarget(taskMemoryManager), overAcquiredRatio);
    }
    return target;
  }

  private static NativeMemoryManager createNativeMemoryManager(
      String name, MemoryTarget target, TaskMemoryMetrics taskMemoryMetrics) {
    ManagedReservationListener rl = new ManagedReservationListener(target, taskMemoryMetrics);
    return NativeMemoryManager.create(name, rl);
  }
}
