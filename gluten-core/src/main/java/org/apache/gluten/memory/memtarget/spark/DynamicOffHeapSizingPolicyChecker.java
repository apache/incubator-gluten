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
package org.apache.gluten.memory.memtarget.spark;

import org.apache.gluten.GlutenConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public final class DynamicOffHeapSizingPolicyChecker {
  private static final Logger LOG =
      LoggerFactory.getLogger(DynamicOffHeapSizingPolicyChecker.class);
  private final long maxOnHeapMemoryInBytes = GlutenConfig.getConf().onHeapMemorySize();
  private final AtomicLong usedOffHeapBytes = new AtomicLong();

  DynamicOffHeapSizingPolicyChecker() {}

  public boolean canBorrow(long size) {
    if (size == 0) {
      return true;
    }

    long totalMemory = Runtime.getRuntime().totalMemory();
    long freeMemory = Runtime.getRuntime().freeMemory();
    long usedOnHeapBytes = (totalMemory - freeMemory);
    long usedOffHeapBytesNow = this.usedOffHeapBytes.get();

    if (size + usedOffHeapBytesNow + usedOnHeapBytes > maxOnHeapMemoryInBytes) {
      LOG.warn(
          String.format(
              "Failing allocation as unified memory is OOM. "
                  + "Used Off-heap: %d, Used On-Heap: %d,"
                  + "Free On-heap: %d, Total On-heap: %d,"
                  + "Max On-heap: %d, Allocation: %d.",
              usedOffHeapBytesNow,
              usedOnHeapBytes,
              freeMemory,
              totalMemory,
              maxOnHeapMemoryInBytes,
              size));

      return false;
    }

    return true;
  }

  public void borrow(long size) {
    usedOffHeapBytes.addAndGet(size);
  }

  public void repay(long size) {
    usedOffHeapBytes.addAndGet(-size);
  }
}
