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
import io.glutenproject.memory.MemoryUsageRecorder;
import io.glutenproject.memory.memtarget.MemoryTarget;
import io.glutenproject.memory.memtarget.MemoryTargets;
import io.glutenproject.memory.memtarget.spark.Spiller;
import io.glutenproject.memory.memtarget.spark.Spillers;
import io.glutenproject.proto.MemoryUsageStats;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.spark.util.TaskResources;

import java.util.Collections;
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
    final double overAcquiredRatio = GlutenConfig.getConf().memoryOverAcquiredRatio();
    final long reservationBlockSize = GlutenConfig.getConf().memoryReservationBlockSize();
    final MemoryTarget target =
        MemoryTargets.throwOnOom(
            MemoryTargets.overAcquire(
                MemoryTargets.newConsumer(
                    TaskResources.getLocalTaskContext().taskMemoryManager(),
                    name,
                    // call memory manager's shrink API, if no good then call the spiller
                    Spillers.withOrder(
                        Spillers.withMinSpillSize(
                            (size) ->
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
                            reservationBlockSize),
                        // the input spiller, called after nmm.shrink was called
                        Spillers.withMinSpillSize(spiller, reservationBlockSize)),
                    Collections.singletonMap(
                        "single",
                        new MemoryUsageRecorder() {
                          @Override
                          public void inc(long bytes) {
                            // no-op
                          }

                          @Override
                          public long peak() {
                            throw new UnsupportedOperationException("Not implemented");
                          }

                          @Override
                          public long current() {
                            throw new UnsupportedOperationException("Not implemented");
                          }

                          @Override
                          public MemoryUsageStats toStats() {
                            final NativeMemoryManager nmm = getNativeMemoryManager();
                            final byte[] usageProto = nmm.collectMemoryUsage();
                            try {
                              return MemoryUsageStats.parseFrom(usageProto);
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
                                                + "memory manager is created. Try moving any "
                                                + "actions about memory allocation out from the "
                                                + "memory manager constructor."));
                          }
                        })),
                overAcquiredRatio));
    // listener
    ManagedReservationListener rl =
        new ManagedReservationListener(target, TaskResources.getSharedUsage());
    // native memory manager
    out.set(NativeMemoryManager.create(name, rl));
    return out.get();
  }
}
