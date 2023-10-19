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

import io.glutenproject.GlutenConfig;
import io.glutenproject.memory.MemoryUsageStatsBuilder;
import io.glutenproject.memory.memtarget.Spiller;
import io.glutenproject.memory.memtarget.TreeMemoryTarget;

import org.apache.spark.memory.TaskMemoryManager;

import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * A hub to provide memory target instances whose shared size (in the same task) is limited to X, X
 * = executor memory / task slots.
 *
 * <p>Using this to prevent OOMs if the delegated memory target could possibly hold large memory
 * blocks that are not spillable.
 *
 * <p>See <a href="https://github.com/oap-project/gluten/issues/3030">GLUTEN-3030</a>
 */
public class IsolatedMemoryConsumers {
  private static final WeakHashMap<TaskMemoryManager, TreeMemoryTarget> MAP = new WeakHashMap<>();

  private IsolatedMemoryConsumers() {}

  private static TreeMemoryTarget getSharedAccount(TaskMemoryManager tmm) {
    synchronized (MAP) {
      return MAP.computeIfAbsent(
          tmm,
          m -> {
            TreeMemoryTarget tmc = new TreeMemoryConsumer(m);
            return tmc.newChild(
                "root",
                GlutenConfig.getConf().conservativeTaskOffHeapMemorySize(),
                Spiller.NO_OP,
                Collections.emptyMap());
          });
    }
  }

  public static TreeMemoryTarget newConsumer(
      TaskMemoryManager tmm,
      String name,
      Spiller spiller,
      Map<String, MemoryUsageStatsBuilder> virtualChildren) {
    TreeMemoryTarget account = getSharedAccount(tmm);
    return account.newChild(name, TreeMemoryConsumer.CAPACITY_UNLIMITED, spiller, virtualChildren);
  }
}
