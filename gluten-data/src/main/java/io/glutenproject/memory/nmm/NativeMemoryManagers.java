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
import io.glutenproject.memory.MemoryUsageStatsBuilder;
import io.glutenproject.memory.SimpleMemoryUsageRecorder;
import io.glutenproject.memory.memtarget.MemoryTarget;
import io.glutenproject.memory.memtarget.MemoryTargets;
import io.glutenproject.memory.memtarget.spark.GlutenMemoryConsumer;
import io.glutenproject.memory.memtarget.spark.Spiller;
import io.glutenproject.memory.memtarget.spark.Spillers;
import io.glutenproject.proto.MemoryUsageStats;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.TaskResources;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public final class NativeMemoryManagers {

  // TODO: Let all caller support spill.
  public static NativeMemoryManager contextInstance(String name) {
    if (!TaskResources.inSparkTask()) {
      throw new IllegalStateException("This method must be called in a Spark task.");
    }
    return TaskResources.addResourceIfNotRegistered(
        name, () -> createNativeMemoryManager(name, Spiller.NO_OP));
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

    final NativeMemoryManager manager = createNativeMemoryManager(name, spiller);
    return TaskResources.addAnonymousResource(manager);
  }

  private static NativeMemoryManager createNativeMemoryManager(String name, Spiller spiller) {
    final AtomicReference<NativeMemoryManager> out = new AtomicReference<>();
    // memory target
    double overAcquiredRatio = GlutenConfig.getConf().veloxOverAcquiredMemoryRatio();
    MemoryTarget target =
        MemoryTargets.throwOnOom(
            MemoryTargets.overAcquire(
                MemoryTargets.newConsumer(
                    name,
                    // call memory manager's shrink API, if no good then call the spiller
                    Spillers.withOrder(
                        (size, trigger) ->
                            Optional.of(out.get())
                                .map(nmm -> nmm.shrink(size))
                                .orElseThrow(
                                    () ->
                                        new IllegalStateException(
                                            ""
                                                + "Shrink is requested before native "
                                                + "memory manager is created. Try moving any "
                                                + "actions about memory allocation out "
                                                + "from the memory manager constructor.")),
                        spiller // the input spiller, called after nmm.shrink was called
                        ),
                    new MemoryUsageStatsBuilder() {
                      private final SimpleMemoryUsageRecorder rootRecorder =
                          new SimpleMemoryUsageRecorder();

                      @Override
                      public void inc(long bytes) {
                        rootRecorder.inc(bytes);
                      }

                      @Override
                      public MemoryUsageStats toStats() {
                        final NativeMemoryManager nmm = getNativeMemoryManager();
                        final byte[] usageProto = nmm.collectMemoryUsage();
                        try {
                          final MemoryUsageStats stats = MemoryUsageStats.parseFrom(usageProto);
                          // Replace the root stats with the one recorded from Java side.
                          //
                          // Root stats returned from C++ (currently) may not include all
                          // allocations
                          // according to the way collectMemoryUsage() is implemented.
                          return MemoryUsageStats.newBuilder()
                              .setCurrent(rootRecorder.current())
                              .setPeak(rootRecorder.peak())
                              .putAllChildren(stats.getChildrenMap())
                              .build();
                        } catch (InvalidProtocolBufferException e) {
                          throw new RuntimeException(e);
                        }
                      }

                      private NativeMemoryManager getNativeMemoryManager() {
                        return Optional.of(out.get())
                            .orElseThrow(
                                () ->
                                    new IllegalStateException(
                                        ""
                                            + "Memory usage stats are requested before native "
                                            + "memory manager is created. Try moving any actions "
                                            + "about memory allocation out from the memory manager "
                                            + "constructor."));
                      }
                    }),
                overAcquiredRatio));
    // listener
    ManagedReservationListener rl =
        new ManagedReservationListener(target, TaskResources.getSharedUsage());
    // native memory manager
    out.set(NativeMemoryManager.create(name, rl));
    return out.get();
  }
}
