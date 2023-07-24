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

import com.google.common.base.Preconditions;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;

import java.io.IOException;

public class OverAcquire implements MemoryTarget {

  // The underlying target.
  private final MemoryTarget target;

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
  //   procedures that were not perfectly-designed. For example,
  //   reservation for a two-step procedure: step A is capable for
  //   spilling while step B is not. If not reserving enough memory
  //   for step B before it's started, it might raise OOM since step A
  //   is ended and no longer open for spilling. In this case the
  //   over-acquired memory will be used in step B.
  private final double ratio;

  public OverAcquire(MemoryTarget target, MemoryTarget overTarget, double ratio) {
    Preconditions.checkArgument(ratio >= 0.0D);
    this.overTarget = overTarget;
    this.target = target;
    this.ratio = ratio;
  }

  @Override
  public long borrow(long size) {
    Preconditions.checkArgument(size != 0, "Size to borrow is zero");
    long granted = target.borrow(size);
    long majorSize = target.bytes();
    long expectedOverAcquired = (long) (ratio * majorSize);
    long overAcquired = overTarget.bytes();
    Preconditions.checkArgument(
        expectedOverAcquired >= overAcquired,
        "Expected over acquired size larger than its old value");
    long diff = expectedOverAcquired - overAcquired;
    overTarget.borrow(diff); // we don't have to check the returned value
    return granted;
  }

  @Override
  public long repay(long size) {
    Preconditions.checkArgument(size != 0, "Size to repay is zero");
    long freed = target.repay(size);
    Preconditions.checkArgument(freed == size, "Repaid size is not equal to requested size");
    // clean up the over-acquired target
    long overAcquired = overTarget.bytes();
    long freedOverAcquired = overTarget.repay(overAcquired);
    Preconditions.checkArgument(
        freedOverAcquired == overAcquired,
        "Freed over-acquired size is not equal to requested size");
    Preconditions.checkArgument(overTarget.bytes() == 0, "Over-acquired target was not cleaned up");
    return size;
  }

  @Override
  public long bytes() {
    return target.bytes();
  }

  public static class DummyTarget extends MemoryConsumer implements MemoryTarget {
    public DummyTarget(TaskMemoryManager taskMemoryManager) {
      super(taskMemoryManager, MemoryMode.OFF_HEAP);
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
      return repay(size);
    }

    @Override
    public long borrow(long size) {
      return acquireMemory(size);
    }

    @Override
    public long repay(long size) {
      long toFree = Math.min(size, getUsed());
      freeMemory(toFree);
      Preconditions.checkArgument(getUsed() >= 0);
      return toFree;
    }

    @Override
    public long bytes() {
      return getUsed();
    }
  }
}
