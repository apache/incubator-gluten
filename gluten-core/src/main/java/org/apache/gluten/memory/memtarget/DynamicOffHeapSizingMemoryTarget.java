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
package org.apache.gluten.memory.memtarget;

import org.apache.gluten.config.GlutenConfig;

import org.apache.spark.annotation.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

@Experimental
public class DynamicOffHeapSizingMemoryTarget implements MemoryTarget {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicOffHeapSizingMemoryTarget.class);
  private final MemoryTarget delegated;
  // When dynamic off-heap sizing is enabled, the off-heap should be sized for the total usable
  // memory, so we can use it as the max memory we will use.
  private static final long MAX_MEMORY_IN_BYTES = GlutenConfig.get().offHeapMemorySize();
  private static final AtomicLong USED_OFFHEAP_BYTES = new AtomicLong();

  public DynamicOffHeapSizingMemoryTarget(MemoryTarget delegated) {
    this.delegated = delegated;
  }

  @Override
  public long borrow(long size) {
    if (size == 0) {
      return 0;
    }

    long totalMemory = Runtime.getRuntime().totalMemory();
    long freeMemory = Runtime.getRuntime().freeMemory();
    long usedOnHeapBytes = (totalMemory - freeMemory);
    long usedOffHeapBytesNow = USED_OFFHEAP_BYTES.get();

    if (size + usedOffHeapBytesNow + usedOnHeapBytes > MAX_MEMORY_IN_BYTES) {
      LOG.warn(
          String.format(
              "Failing allocation as unified memory is OOM. "
                  + "Used Off-heap: %d, Used On-Heap: %d, "
                  + "Free On-heap: %d, Total On-heap: %d, "
                  + "Max On-heap: %d, Allocation: %d.",
              usedOffHeapBytesNow,
              usedOnHeapBytes,
              freeMemory,
              totalMemory,
              MAX_MEMORY_IN_BYTES,
              size));

      return 0;
    }

    long reserved = delegated.borrow(size);

    USED_OFFHEAP_BYTES.addAndGet(reserved);

    return reserved;
  }

  @Override
  public long repay(long size) {
    long unreserved = delegated.repay(size);

    USED_OFFHEAP_BYTES.addAndGet(-unreserved);

    return unreserved;
  }

  @Override
  public long usedBytes() {
    return delegated.usedBytes();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  public MemoryTarget delegated() {
    return delegated;
  }
}
