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

import io.glutenproject.memory.MemoryUsage;
import io.glutenproject.memory.MemoryUsageStats;
import io.glutenproject.memory.memtarget.TaskManagedMemoryTarget;

import com.google.common.base.Preconditions;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.TaskResources;

public class GlutenMemoryConsumer extends MemoryConsumer implements TaskManagedMemoryTarget {
  private final TaskMemoryManager taskMemoryManager;
  private final Spiller spiller;
  private final String name;
  private final MemoryUsage usage = new MemoryUsage();

  public GlutenMemoryConsumer(String name, TaskMemoryManager taskMemoryManager, Spiller spiller) {
    super(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.OFF_HEAP);
    this.taskMemoryManager = taskMemoryManager;
    this.spiller = spiller;
    this.name = name;
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
    assert size > 0;
    long acquired = acquireMemory(size);
    if (acquired < size) {
      this.taskMemoryManager.showMemoryUsage();
    }
    usage.inc(acquired);
    return acquired;
  }

  public long free(long size) {
    assert size > 0;
    freeMemory(size);
    Preconditions.checkArgument(getUsed() >= 0);
    usage.inc(-size);
    return size;
  }

  @Override
  public String name() {
    return "GlutenMemoryConsumer/" + name;
  }

  @Override
  public MemoryUsageStats stats() {
    MemoryUsageStats stats = this.usage.toStats();
    Preconditions.checkState(
        stats.current == getUsed(),
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
