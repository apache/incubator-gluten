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
import org.apache.gluten.memory.memtarget.TreeMemoryTarget;

import org.apache.spark.TaskContext;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.util.TaskResources$;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import scala.Function0;

public class TreeMemoryConsumerTest {
  @Test
  public void testIsolated() {
    final SQLConf conf = new SQLConf();
    conf.setConfString(
        GlutenConfig.COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES().key(), "100");
    test(
        conf,
        () -> {
          final TreeMemoryConsumers.Factory factory = TreeMemoryConsumers.isolated();
          final TreeMemoryTarget consumer =
              factory.newConsumer(
                  TaskContext.get().taskMemoryManager(),
                  "FOO",
                  Collections.emptyList(),
                  Collections.emptyMap());
          Assert.assertEquals(20, consumer.borrow(20));
          Assert.assertEquals(70, consumer.borrow(70));
          Assert.assertEquals(10, consumer.borrow(20));
          Assert.assertEquals(0, consumer.borrow(20));
        });
  }

  @Test
  public void testShared() {
    final SQLConf conf = new SQLConf();
    conf.setConfString(
        GlutenConfig.COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES().key(), "100");
    test(
        conf,
        () -> {
          final TreeMemoryConsumers.Factory factory = TreeMemoryConsumers.shared();
          final TreeMemoryTarget consumer =
              factory.newConsumer(
                  TaskContext.get().taskMemoryManager(),
                  "FOO",
                  Collections.emptyList(),
                  Collections.emptyMap());
          Assert.assertEquals(20, consumer.borrow(20));
          Assert.assertEquals(70, consumer.borrow(70));
          Assert.assertEquals(20, consumer.borrow(20));
          Assert.assertEquals(20, consumer.borrow(20));
        });
  }

  @Test
  public void testIsolatedAndShared() {
    final SQLConf conf = new SQLConf();
    conf.setConfString(
        GlutenConfig.COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES().key(), "100");
    test(
        conf,
        () -> {
          final TreeMemoryTarget shared =
              TreeMemoryConsumers.shared()
                  .newConsumer(
                      TaskContext.get().taskMemoryManager(),
                      "FOO",
                      Collections.emptyList(),
                      Collections.emptyMap());
          Assert.assertEquals(110, shared.borrow(110));
          final TreeMemoryTarget isolated =
              TreeMemoryConsumers.isolated()
                  .newConsumer(
                      TaskContext.get().taskMemoryManager(),
                      "FOO",
                      Collections.emptyList(),
                      Collections.emptyMap());
          Assert.assertEquals(100, isolated.borrow(110));
        });
  }

  private void test(SQLConf conf, Runnable r) {
    TaskResources$.MODULE$.runUnsafe(
        new Function0<Object>() {
          @Override
          public Object apply() {
            SQLConf.withExistingConf(
                conf,
                new Function0<Object>() {
                  @Override
                  public Object apply() {
                    r.run();
                    return null;
                  }
                });
            return null;
          }
        });
  }
}
