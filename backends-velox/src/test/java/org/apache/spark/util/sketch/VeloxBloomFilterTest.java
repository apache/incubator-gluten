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
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class VeloxBloomFilterTest {

  @BeforeClass
  public static void setup() {
    JniWorkspace.enableDebug();
    final ListenerApi api = new ListenerApiImpl();
    api.onDriverStart(new SparkConf());
  }

  @Test
  public void testEmpty() {
    final InputStream in = new ByteArrayInputStream(new byte[0]);
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final BloomFilter filter = VeloxBloomFilter.readFrom(in);
          // todo fake some data to test
          return null;
        });
  }
}
