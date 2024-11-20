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
package org.apache.gluten.test;

import org.apache.gluten.GlutenConfig;
import org.apache.gluten.backendsapi.ListenerApi;
import org.apache.gluten.backendsapi.velox.VeloxListenerApi;

import com.codahale.metrics.MetricRegistry;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.resource.ResourceInformation;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Map;

/** For testing Velox backend without starting a Spark context. */
public abstract class VeloxBackendTestBase {
  @BeforeClass
  public static void setup() {
    final ListenerApi api = new VeloxListenerApi();
    api.onDriverStart(mockSparkContext(), mockPluginContext());
  }

  private static SparkContext mockSparkContext() {
    // Not yet implemented.
    return null;
  }

  private static PluginContext mockPluginContext() {
    return new PluginContext() {
      @Override
      public MetricRegistry metricRegistry() {
        throw new UnsupportedOperationException();
      }

      @Override
      public SparkConf conf() {
        final SparkConf conf = new SparkConf();
        conf.set(GlutenConfig.COLUMNAR_VELOX_CONNECTOR_IO_THREADS().key(), "0");
        conf.set(GlutenConfig.GLUTEN_OFFHEAP_SIZE_KEY(), "1g");
        return conf;
      }

      @Override
      public String executorID() {
        throw new UnsupportedOperationException();
      }

      @Override
      public String hostname() {
        throw new UnsupportedOperationException();
      }

      @Override
      public Map<String, ResourceInformation> resources() {
        throw new UnsupportedOperationException();
      }

      @Override
      public void send(Object message) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public Object ask(Object message) throws Exception {
        throw new UnsupportedOperationException();
      }
    };
  }
}
