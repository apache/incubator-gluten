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
import io.glutenproject.memory.SimpleMemoryUsageRecorder;
import io.glutenproject.memory.alloc.NativeMemoryAllocators;
import io.glutenproject.memory.memtarget.MemoryTarget;
import io.glutenproject.memory.memtarget.MemoryTargets;
import io.glutenproject.memory.memtarget.spark.GlutenMemoryConsumer;
import io.glutenproject.memory.memtarget.spark.Spiller;
import io.glutenproject.memory.memtarget.spark.Spillers;
import io.glutenproject.proto.MemoryUsageStats;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.TaskResource;
import org.apache.spark.util.TaskResources;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class NativeMemoryManager implements TaskResource {

  private static final Logger LOGGER = LoggerFactory.getLogger(NativeMemoryManager.class);

  private final long nativeInstanceId;
  private final String name;
  private final ReservationListener listener;

  private NativeMemoryManager(String name, long nativeInstanceId, ReservationListener listener) {
    this.name = name;
    this.nativeInstanceId = nativeInstanceId;
    this.listener = listener;
  }

  public static NativeMemoryManager create(String name, ReservationListener listener) {
    long allocatorId = NativeMemoryAllocators.getDefault().globalInstance().getNativeInstanceId();
    long reservationBlockSize = GlutenConfig.getConf().veloxReservationBlockSize();
    return new NativeMemoryManager(
        name, create(name, allocatorId, reservationBlockSize, listener), listener);
  }

  public long getNativeInstanceId() {
    return this.nativeInstanceId;
  }

  public byte[] collectMemoryUsage() {
    return collectMemoryUsage(nativeInstanceId);
  }

  public long shrink(long size) {
    return shrink(nativeInstanceId, size);
  }

  private static native long shrink(long nativeInstanceId, long size);

  private static native long create(
      String name, long allocatorId, long reservationBlockSize, ReservationListener listener);

  private static native void release(long memoryManagerId);

  private static native byte[] collectMemoryUsage(long memoryManagerId);

  @Override
  public void release() throws Exception {
    release(nativeInstanceId);
    if (listener.getUsedBytes() != 0) {
      LOGGER.warn(
          name
              + " Reservation listener still reserved non-zero bytes, which may cause "
              + "memory leak, size: "
              + Utils.bytesToString(listener.getUsedBytes()));
    }
  }

  @Override
  public int priority() {
    return 0; // lowest release priority
  }

  @Override
  public String resourceName() {
    return name + "_mem";
  }

  public static class Builder {
    private final String name;
    private Spiller spiller = Spiller.NO_OP;

    public Builder(String name) {
      this.name = name;
    }

    public Builder setSpiller(Spiller spiller) {
      this.spiller = spiller;
      return this;
    }

    private NativeMemoryManager createNativeMemoryManager() {
      final AtomicReference<NativeMemoryManager> out = new AtomicReference<>();
      // memory target
      TaskMemoryManager taskMemoryManager = TaskContext.get().taskMemoryManager();
      double overAcquiredRatio = GlutenConfig.getConf().veloxOverAcquiredMemoryRatio();
      MemoryTarget target =
          MemoryTargets.throwOnOom(
              MemoryTargets.overAcquire(
                  new GlutenMemoryConsumer(
                      this.name,
                      taskMemoryManager,
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
                          this.spiller // the input spiller, called after nmm.shrink was called
                      ),
                      new GlutenMemoryConsumer.StatsBuilder() {
                        private final SimpleMemoryUsageRecorder rootRecorder =
                            new SimpleMemoryUsageRecorder();

                        @Override
                        public void onAcquire(long size) {
                          rootRecorder.inc(size);
                        }

                        @Override
                        public void onFree(long size) {
                          rootRecorder.inc(-size);
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
                                          "Memory usage stats are requested"
                                              + " before native memory manager is created. "
                                              + "Try moving any actions about memory allocation"
                                              + " out from the memory manager constructor."));
                        }
                      }),
                  overAcquiredRatio));
      // listener
      ManagedReservationListener rl =
          new ManagedReservationListener(target, TaskResources.getSharedUsage());
      // native memory manager
      out.set(NativeMemoryManager.create(this.name, rl));
      return out.get();
    }

    public NativeMemoryManager build() {
      if (!TaskResources.inSparkTask()) {
        throw new IllegalStateException("This method must be called in a Spark task.");
      }
      return TaskResources.addResourceIfNotRegistered(this.name, this::createNativeMemoryManager);
    }

    /**
     * Build a temporary memory manager, caller should call NativeMemoryManager#release manually.
     */
    public NativeMemoryManager buildTempInstance() {
      if (TaskResources.inSparkTask()) {
        throw new IllegalStateException("This method should not used here.");
      }
      return NativeMemoryManager.create(this.name, ReservationListener.NOOP);
    }
  }
}
