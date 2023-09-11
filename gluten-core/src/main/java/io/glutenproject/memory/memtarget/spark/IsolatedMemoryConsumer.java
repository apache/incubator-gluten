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

import io.glutenproject.memory.memtarget.TaskMemoryTarget;
import io.glutenproject.proto.MemoryUsageStats;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;

import java.io.IOException;

/**
 * This is a Spark memory consumer and at the same time a factory to create sub-targets that
 * share one fixed memory capacity.
 * <p>
 * Typically used by memory target {@link io.glutenproject.memory.memtarget.IsolatedByTaskSlot}
 */
public class IsolatedMemoryConsumer extends MemoryConsumer implements TaskMemoryTarget {
  private IsolatedMemoryConsumer(TaskMemoryManager taskMemoryManager, long capacity) {
    super(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.OFF_HEAP);
    // todo
  }

  @Override
  public long borrow(long size) {
    return 0;
  }

  @Override
  public long repay(long size) {
    return 0;
  }

  @Override
  public String name() {
    return null;
  }

  @Override
  public long usedBytes() {
    return 0;
  }

  @Override
  public MemoryUsageStats stats() {
    return null;
  }

  @Override
  public TaskMemoryManager getTaskMemoryManager() {
    return null;
  }

  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    return 0;
  }
}
