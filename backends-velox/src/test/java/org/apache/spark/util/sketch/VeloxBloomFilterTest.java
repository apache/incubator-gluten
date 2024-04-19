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
package org.apache.spark.util.sketch;

import org.apache.gluten.backendsapi.ListenerApi;
import org.apache.gluten.backendsapi.velox.ListenerApiImpl;
import org.apache.gluten.vectorized.JniWorkspace;

import org.apache.spark.SparkConf;
import org.apache.spark.util.TaskResources$;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;

public class VeloxBloomFilterTest {

  @BeforeClass
  public static void setup() {
    JniWorkspace.enableDebug();
    final ListenerApi api = new ListenerApiImpl();
    api.onDriverStart(new SparkConf());
  }

  @Test
  public void testEmpty1() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final BloomFilter filter = VeloxBloomFilter.empty(10000);
          for (int i = 0; i < 1000; i++) {
            Assert.assertFalse(filter.mightContainLong(i));
          }
          return null;
        });
  }

  @Test
  public void testEmpty2() {
    final ByteBuffer buf = ByteBuffer.allocate(5);
    buf.put((byte) 1); // kBloomFilterV1
    buf.putInt(0); // size
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final BloomFilter filter = VeloxBloomFilter.readFrom(buf.array());
          for (int i = 0; i < 1000; i++) {
            Assert.assertFalse(filter.mightContainLong(i));
          }
          return null;
        });
  }

  @Test
  public void testSanity() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final BloomFilter filter = VeloxBloomFilter.empty(10000);
          int numItems = 1000;
          for (int i = 0; i < numItems; i++) {
            Assert.assertFalse(filter.mightContainLong(i));
          }
          for (int i = 0; i < numItems; i++) {
            filter.putLong(i);
            Assert.assertTrue(filter.mightContainLong(i));
          }
          for (int i = 0; i < numItems; i++) {
            Assert.assertTrue(filter.mightContainLong(i));
          }

          return null;
        });
  }

  @Test
  public void testSerde() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final BloomFilter filter = VeloxBloomFilter.empty(10000);
          for (int i = 0; i < 1000; i++) {
            filter.putLong(i);
          }

          final ByteArrayOutputStream o = new ByteArrayOutputStream();
          try {
            filter.writeTo(o);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          byte[] data1 = o.toByteArray();

          final BloomFilter filter2 = VeloxBloomFilter.readFrom(data1);
          final ByteArrayOutputStream o2 = new ByteArrayOutputStream();
          try {
            filter2.writeTo(o2);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          byte[] data2 = o2.toByteArray();

          Assert.assertArrayEquals(data2, data1);
          return null;
        });
  }
}
