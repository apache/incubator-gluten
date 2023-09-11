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

import io.glutenproject.memory.MemoryUsageStatsBuilder;
import io.glutenproject.memory.memtarget.spark.RegularMemoryConsumer;
import io.glutenproject.memory.memtarget.spark.Spiller;

import org.apache.spark.memory.TaskMemoryManager;

public final class MemoryTargets {

  private MemoryTargets() {
    // enclose factory ctor
  }

  public static MemoryTarget throwOnOom(TaskMemoryTarget target) {
    return new ThrowOnOomMemoryTarget(target);
  }

  public static TaskMemoryTarget overAcquire(TaskMemoryTarget target, double overAcquiredRatio) {
    if (overAcquiredRatio == 0.0D) {
      return target;
    }
    return new OverAcquire(target, overAcquiredRatio);
  }

  public static TaskMemoryTarget newConsumer(
      TaskMemoryManager tmm, String name, Spiller spiller, MemoryUsageStatsBuilder statsBuilder) {
    final RegularMemoryConsumer gmc = new RegularMemoryConsumer(tmm, name, spiller, statsBuilder);
    return gmc;
  }
}
