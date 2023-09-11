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
package io.glutenproject.memory.memtarget.spark;

import io.glutenproject.memory.MemoryUsageStatsBuilder;
import io.glutenproject.memory.memtarget.TaskMemoryTarget;
import io.glutenproject.proto.MemoryUsageStats;

import com.google.common.base.Preconditions;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.TaskResources;

/**
 * A trivial memory consumer implementation used by Gluten.
 */
public class RegularMemoryConsumer extends MemoryConsumer implements TaskMemoryTarget {
  private final TaskMemoryManager taskMemoryManager;
  private final Spiller spiller;
  private final String name;
  private final MemoryUsageStatsBuilder statsBuilder;

  public RegularMemoryConsumer(
      TaskMemoryManager taskMemoryManager,
      String name,
      Spiller spiller,
      MemoryUsageStatsBuilder statsBuilder) {
    super(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.OFF_HEAP);
    this.taskMemoryManager = taskMemoryManager;
    this.spiller = spiller;
    this.name = name;
    this.statsBuilder = statsBuilder;
  }

  @Override
  public long spill(long size, MemoryConsumer trigger) {
    long spilledOut = spiller.spill(size, trigger);
    if (TaskResources.inSparkTask()) {
      TaskResources.getLocalTaskContext().taskMetrics().incMemoryBytesSpilled(spilledOut);
    }
    return spilledOut;
  }

  @Override
  public TaskMemoryManager getTaskMemoryManager() {
    return taskMemoryManager;
  }

  public long acquire(long size) {
    if (size == 0) {
      // or Spark complains the zero size by throwing an error
      return 0;
    }
    long acquired = acquireMemory(size);
    statsBuilder.inc(acquired);
    return acquired;
  }

  public long free(long size) {
    if (size == 0) {
      return 0;
    }
    freeMemory(size);
    Preconditions.checkArgument(getUsed() >= 0);
    statsBuilder.inc(-size);
    return size;
  }

  @Override
  public String name() {
    return String.format(
        "Gluten.Regular.%s@%s", name, Integer.toHexString(System.identityHashCode(this)));
  }

  @Override
  public long usedBytes() {
    return getUsed();
  }

  @Override
  public MemoryUsageStats stats() {
    MemoryUsageStats stats = this.statsBuilder.toStats();
    Preconditions.checkState(
        stats.getCurrent() == getUsed(),
        "Used bytes mismatch between gluten memory consumer and Spark task memory manager");
    return stats;
  }

  @Override
  public long borrow(long size) {
    return acquire(size);
  }

  @Override
  public long repay(long size) {
    return free(size);
  }

  @Override
  public String toString() {
    return name();
  }
}
