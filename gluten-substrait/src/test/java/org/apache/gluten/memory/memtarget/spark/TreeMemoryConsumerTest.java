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
import org.apache.gluten.memory.memtarget.MemoryTarget;
import org.apache.gluten.memory.memtarget.Spiller;
import org.apache.gluten.memory.memtarget.Spillers;
import org.apache.gluten.memory.memtarget.TreeMemoryTarget;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.task.TaskResources$;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import scala.Function0;

public class TreeMemoryConsumerTest {
  @Before
  public void setUp() throws Exception {
    final SQLConf conf = SQLConf.get();
    conf.setConfString("spark.memory.offHeap.enabled", "true");
    conf.setConfString("spark.memory.offHeap.size", "400");
    conf.setConfString(
        GlutenConfig.COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES().key(), "100");
  }

  @Test
  public void testIsolated() {
    test(
        () -> {
          final TreeMemoryConsumers.Factory factory = TreeMemoryConsumers.isolated();
          final TreeMemoryTarget consumer =
              factory.newConsumer(
                  TaskContext.get().taskMemoryManager(),
                  "FOO",
                  Spillers.NOOP,
                  Collections.emptyMap());
          Assert.assertEquals(20, consumer.borrow(20));
          Assert.assertEquals(70, consumer.borrow(70));
          Assert.assertEquals(10, consumer.borrow(20));
          Assert.assertEquals(0, consumer.borrow(20));
        });
  }

  @Test
  public void testShared() {
    test(
        () -> {
          final TreeMemoryConsumers.Factory factory = TreeMemoryConsumers.shared();
          final TreeMemoryTarget consumer =
              factory.newConsumer(
                  TaskContext.get().taskMemoryManager(),
                  "FOO",
                  Spillers.NOOP,
                  Collections.emptyMap());
          Assert.assertEquals(20, consumer.borrow(20));
          Assert.assertEquals(70, consumer.borrow(70));
          Assert.assertEquals(20, consumer.borrow(20));
          Assert.assertEquals(20, consumer.borrow(20));
        });
  }

  @Test
  public void testIsolatedAndShared() {
    test(
        () -> {
          final TreeMemoryTarget shared =
              TreeMemoryConsumers.shared()
                  .newConsumer(
                      TaskContext.get().taskMemoryManager(),
                      "FOO",
                      Spillers.NOOP,
                      Collections.emptyMap());
          Assert.assertEquals(110, shared.borrow(110));
          final TreeMemoryTarget isolated =
              TreeMemoryConsumers.isolated()
                  .newConsumer(
                      TaskContext.get().taskMemoryManager(),
                      "FOO",
                      Spillers.NOOP,
                      Collections.emptyMap());
          Assert.assertEquals(100, isolated.borrow(110));
        });
  }

  @Test
  public void testSpill() {
    test(
        () -> {
          final Spillers.AppendableSpillerList spillers = Spillers.appendable();
          final TreeMemoryTarget shared =
              TreeMemoryConsumers.shared()
                  .newConsumer(
                      TaskContext.get().taskMemoryManager(),
                      "FOO",
                      spillers,
                      Collections.emptyMap());
          final AtomicInteger numSpills = new AtomicInteger(0);
          final AtomicLong numSpilledBytes = new AtomicLong(0L);
          spillers.append(
              new Spiller() {
                @Override
                public long spill(MemoryTarget self, Phase phase, long size) {
                  long repaid = shared.repay(size);
                  numSpills.getAndIncrement();
                  numSpilledBytes.getAndAdd(repaid);
                  return repaid;
                }
              });
          Assert.assertEquals(300, shared.borrow(300));
          Assert.assertEquals(300, shared.borrow(300));
          Assert.assertEquals(1, numSpills.get());
          Assert.assertEquals(200, numSpilledBytes.get());
          Assert.assertEquals(400, shared.usedBytes());

          Assert.assertEquals(300, shared.borrow(300));
          Assert.assertEquals(300, shared.borrow(300));
          Assert.assertEquals(3, numSpills.get());
          Assert.assertEquals(800, numSpilledBytes.get());
          Assert.assertEquals(400, shared.usedBytes());
        });
  }

  @Test
  public void testOverSpill() {
    test(
        () -> {
          final Spillers.AppendableSpillerList spillers = Spillers.appendable();
          final TreeMemoryTarget shared =
              TreeMemoryConsumers.shared()
                  .newConsumer(
                      TaskContext.get().taskMemoryManager(),
                      "FOO",
                      spillers,
                      Collections.emptyMap());
          final AtomicInteger numSpills = new AtomicInteger(0);
          final AtomicLong numSpilledBytes = new AtomicLong(0L);
          spillers.append(
              new Spiller() {
                @Override
                public long spill(MemoryTarget self, Phase phase, long size) {
                  long repaid = shared.repay(Long.MAX_VALUE);
                  numSpills.getAndIncrement();
                  numSpilledBytes.getAndAdd(repaid);
                  return repaid;
                }
              });
          Assert.assertEquals(300, shared.borrow(300));
          Assert.assertEquals(300, shared.borrow(300));
          Assert.assertEquals(1, numSpills.get());
          Assert.assertEquals(300, numSpilledBytes.get());
          Assert.assertEquals(300, shared.usedBytes());

          Assert.assertEquals(300, shared.borrow(300));
          Assert.assertEquals(300, shared.borrow(300));
          Assert.assertEquals(3, numSpills.get());
          Assert.assertEquals(900, numSpilledBytes.get());
          Assert.assertEquals(300, shared.usedBytes());
        });
  }

  private void test(Runnable r) {
    TaskResources$.MODULE$.runUnsafe(
        new Function0<Object>() {
          @Override
          public Object apply() {
            r.run();
            return null;
          }
        });
  }
}
