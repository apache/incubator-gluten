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

package io.glutenproject.memory;

import java.io.IOException;

import io.glutenproject.memory.alloc.Spiller;

import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;

public class GlutenMemoryConsumer extends MemoryConsumer {

  protected final Spiller spiller;

  public GlutenMemoryConsumer(TaskMemoryManager taskMemoryManager, Spiller spiller) {
    super(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.OFF_HEAP);
    this.spiller = spiller;
  }

  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    return spiller.spill(size, trigger);
  }

  public long acquire(long size) {
    if (size <= 0L) {
      return 0L;
    }
    return acquireMemory(size);
  }

  public long free(long size) {
    freeMemory(size);
    return size;
  }
}
