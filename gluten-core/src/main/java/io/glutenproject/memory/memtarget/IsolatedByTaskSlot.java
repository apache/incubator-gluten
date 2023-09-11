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

import io.glutenproject.proto.MemoryUsageStats;

import org.apache.spark.memory.TaskMemoryManager;

// A decorator to a task memory target, to restrict memory usage of the delegated
//   memory target to X, X = free executor memory / task slots.
// Using this to prevent OOMs if the delegated memory target could possibly
//   hold large memory blocks that are not spillable.
//   See https://github.com/oap-project/gluten/issues/3030
public class IsolatedByTaskSlot implements TaskMemoryTarget {

  private final TaskMemoryTarget delegated;

  public IsolatedByTaskSlot(TaskMemoryTarget delegated) {
    this.delegated = delegated;
  }

  @Override
  public long borrow(long size) {
    // limit within the minimum execution pool cap: (max offheap - max storage) / task slot
    return delegated.borrow(size);
  }

  @Override
  public long repay(long size) {
    return delegated.repay(size);
  }

  @Override
  public String name() {
    return "Isolated." + delegated.name();
  }

  @Override
  public long usedBytes() {
    return delegated.usedBytes();
  }

  @Override
  public MemoryUsageStats stats() {
    return delegated.stats();
  }

  @Override
  public TaskMemoryManager getTaskMemoryManager() {
    return delegated.getTaskMemoryManager();
  }
}
