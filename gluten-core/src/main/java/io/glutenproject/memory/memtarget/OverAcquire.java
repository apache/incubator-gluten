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
package io.glutenproject.memory.memtarget;

import io.glutenproject.memory.SimpleMemoryUsageRecorder;
import io.glutenproject.proto.MemoryUsageStats;

import com.google.common.base.Preconditions;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;

import java.io.IOException;

class OverAcquire implements TaskManagedMemoryTarget {
  private final SimpleMemoryUsageRecorder usage = new SimpleMemoryUsageRecorder();

  // The underlying target.
  private final TaskManagedMemoryTarget target;

  // This consumer holds the over-acquired memory.
  private final MemoryTarget overTarget;

  // The ratio is normally 0.
  //
  // If set to some value other than 0, the consumer will try
  //   over-acquire this ratio of memory each time it acquires
  //   from Spark.
  //
  // Once OOM, the over-acquired memory will be used as backup.
  //
  // The over-acquire is a general workaround for underling reservation
  //   procedures that were not perfectly-designed for spilling. For example,
  //   reservation for a two-step procedure: step A is capable for
  //   spilling while step B is not. If not reserving enough memory
  //   for step B before it's started, it might raise OOM since step A
  //   is ended and no longer open for spilling. In this case the
  //   over-acquired memory will be used in step B.
  private final double ratio;

  OverAcquire(TaskManagedMemoryTarget target, double ratio) {
    Preconditions.checkArgument(ratio >= 0.0D);
    this.overTarget = new OverAcquire.DummyTarget(target.getTaskMemoryManager());
    this.target = target;
    this.ratio = ratio;
  }

  @Override
  public long borrow(long size) {
    Preconditions.checkArgument(size != 0, "Size to borrow is zero");
    long granted = target.borrow(size);
    usage.inc(granted);
    long majorSize = target.usedBytes();
    long expectedOverAcquired = (long) (ratio * majorSize);
    long overAcquired = overTarget.usedBytes();
    Preconditions.checkArgument(
        expectedOverAcquired >= overAcquired,
        "Expected over acquired size larger than its old value");
    long diff = expectedOverAcquired - overAcquired;
    overTarget.borrow(diff); // we don't have to check the returned value
    usage.inc(diff);
    return granted;
  }

  @Override
  public long repay(long size) {
    Preconditions.checkArgument(size != 0, "Size to repay is zero");
    long freed = target.repay(size);
    usage.inc(-freed);
    Preconditions.checkArgument(freed == size, "Repaid size is not equal to requested size");
    // clean up the over-acquired target
    long overAcquired = overTarget.usedBytes();
    long freedOverAcquired = overTarget.repay(overAcquired);
    Preconditions.checkArgument(
        freedOverAcquired == overAcquired,
        "Freed over-acquired size is not equal to requested size");
    Preconditions.checkArgument(
        overTarget.usedBytes() == 0, "Over-acquired target was not cleaned up");
    usage.inc(-freedOverAcquired);
    return size;
  }

  @Override
  public String name() {
    return String.format("OverAcquire-[%s][%s]", target.name(), overTarget.name());
  }

  @Override
  public long usedBytes() {
    return usage.current();
  }

  @Override
  public MemoryUsageStats stats() {
    return usage.toStats();
  }

  @Override
  public TaskMemoryManager getTaskMemoryManager() {
    return target.getTaskMemoryManager();
  }

  private static class DummyTarget extends MemoryConsumer implements MemoryTarget {
    private final SimpleMemoryUsageRecorder usage = new SimpleMemoryUsageRecorder();

    private DummyTarget(TaskMemoryManager taskMemoryManager) {
      super(taskMemoryManager, MemoryMode.OFF_HEAP);
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
      return repay(size);
    }

    @Override
    public long borrow(long size) {
      if (size == 0) {
        // or Spark complains the zero size by throwing an error
        return 0;
      }
      long granted = acquireMemory(size);
      usage.inc(granted);
      return granted;
    }

    @Override
    public long repay(long size) {
      long toFree = Math.min(size, getUsed());
      freeMemory(toFree);
      Preconditions.checkArgument(getUsed() >= 0);
      usage.inc(-toFree);
      return toFree;
    }

    @Override
    public String name() {
      return "OverAcquire.DummyTarget";
    }

    @Override
    public long usedBytes() {
      return getUsed();
    }

    @Override
    public MemoryUsageStats stats() {
      return usage.toStats();
    }
  }
}
