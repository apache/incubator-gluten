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
package org.apache.gluten.memory.listener;

import org.apache.gluten.GlutenConfig;
import org.apache.gluten.memory.MemoryUsageStatsBuilder;
import org.apache.gluten.memory.SimpleMemoryUsageRecorder;
import org.apache.gluten.memory.memtarget.*;

import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.TaskResources;

import java.util.Collections;
import java.util.Map;

public final class ReservationListeners {
  public static final ReservationListener NOOP =
      new ManagedReservationListener(
          new NoopMemoryTarget(), new SimpleMemoryUsageRecorder(), new Object());

  public static ReservationListener create(
      String name, Spiller spiller, Map<String, MemoryUsageStatsBuilder> mutableStats) {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException(
          "Spillable reservation listener must be used in a Spark task.");
    }
    return create0(name, spiller, mutableStats);
  }

  private static ReservationListener create0(
      String name, Spiller spiller, Map<String, MemoryUsageStatsBuilder> mutableStats) {
    // Memory target.
    final double overAcquiredRatio = GlutenConfig.getConf().memoryOverAcquiredRatio();
    final long reservationBlockSize = GlutenConfig.getConf().memoryReservationBlockSize();
    final TaskMemoryManager tmm = TaskResources.getLocalTaskContext().taskMemoryManager();
    final TreeMemoryTarget consumer =
        MemoryTargets.newConsumer(
            tmm, name, Spillers.withMinSpillSize(spiller, reservationBlockSize), mutableStats);
    final MemoryTarget overConsumer =
        MemoryTargets.newConsumer(
            tmm,
            consumer.name() + ".OverAcquire",
            new Spiller() {
              @Override
              public long spill(MemoryTarget self, Phase phase, long size) {
                if (!Spillers.PHASE_SET_ALL.contains(phase)) {
                  return 0L;
                }
                return self.repay(size);
              }
            },
            Collections.emptyMap());
    final MemoryTarget target =
        MemoryTargets.throwOnOom(
            MemoryTargets.overAcquire(
                MemoryTargets.dynamicOffHeapSizingIfEnabled(consumer),
                MemoryTargets.dynamicOffHeapSizingIfEnabled(overConsumer),
                overAcquiredRatio));

    // Listener.
    return new ManagedReservationListener(target, TaskResources.getSharedUsage(), tmm);
  }
}
