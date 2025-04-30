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
import org.apache.gluten.memory.SimpleMemoryUsageRecorder;
import org.apache.gluten.proto.MemoryUsageStats;

import org.apache.spark.annotation.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The memory target used by dynamic off-heap sizing. Since
 * https://github.com/apache/incubator-gluten/issues/5439.
 */
@Experimental
public class DynamicOffHeapSizingMemoryTarget implements MemoryTarget, KnownNameAndStats {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicOffHeapSizingMemoryTarget.class);
  // When dynamic off-heap sizing is enabled, the off-heap should be sized for the total usable
  // memory, so we can use it as the max memory we will use.
  private static final long TOTAL_MEMORY_SHARED;

  static {
    final long maxOnHeapSize = Runtime.getRuntime().maxMemory();
    final double fractionForSizing = GlutenConfig.get().dynamicOffHeapSizingMemoryFraction();
    // Since when dynamic off-heap sizing is enabled, we commingle on-heap
    // and off-heap memory, we set the off-heap size to the usable on-heap size. We will
    // size it with a memory fraction, which can be aggressively set, but the default
    // is using the same way that Spark sizes on-heap memory:
    //
    // spark.gluten.memory.dynamic.offHeap.sizing.memory.fraction *
    //    (spark.executor.memory - 300MB).
    //
    // We will be careful to use the same configuration settings as Spark to ensure
    // that we are sizing the off-heap memory in the same way as Spark sizes on-heap memory.
    // The 300MB value, unfortunately, is hard-coded in Spark code.
    TOTAL_MEMORY_SHARED = (long) ((maxOnHeapSize - (300 * 1024 * 1024)) * fractionForSizing);
    LOG.info("DynamicOffHeapSizingMemoryTarget MAX_MEMORY_IN_BYTES: {}", TOTAL_MEMORY_SHARED);
  }

  private static final AtomicLong USED_OFF_HEAP_BYTES = new AtomicLong();

  private final String name = MemoryTargetUtil.toUniqueName("DynamicOffHeapSizing");
  private final SimpleMemoryUsageRecorder recorder = new SimpleMemoryUsageRecorder();

  public DynamicOffHeapSizingMemoryTarget() {}

  @Override
  public long borrow(long size) {
    if (size == 0) {
      return 0;
    }

    // Only JVM shrinking can reclaim space from the total JVM memory.
    // See https://github.com/apache/incubator-gluten/issues/9276.
    long totalHeapMemory = Runtime.getRuntime().totalMemory();
    long freeHeapMemory = Runtime.getRuntime().freeMemory();
    long usedOffHeapMemory = USED_OFF_HEAP_BYTES.get();

    // Adds the total JVM memory which is the actual memory the JVM occupied from the operating
    // system into the counter.
    if (size + usedOffHeapMemory + totalHeapMemory > TOTAL_MEMORY_SHARED) {
      LOG.warn(
          String.format(
              "Failing allocation as unified memory is OOM. "
                  + "Used Off-heap: %d, Used On-Heap: %d, "
                  + "Free On-heap: %d, Total On-heap: %d, "
                  + "Max On-heap: %d, Allocation: %d.",
              usedOffHeapMemory,
              totalHeapMemory - freeHeapMemory,
              freeHeapMemory,
              totalHeapMemory,
              TOTAL_MEMORY_SHARED,
              size));

      return 0;
    }

    USED_OFF_HEAP_BYTES.addAndGet(size);
    recorder.inc(size);
    return size;
  }

  @Override
  public long repay(long size) {
    USED_OFF_HEAP_BYTES.addAndGet(-size);
    recorder.inc(-size);
    return size;
  }

  @Override
  public long usedBytes() {
    return recorder.current();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public MemoryUsageStats stats() {
    return recorder.toStats();
  }
}
