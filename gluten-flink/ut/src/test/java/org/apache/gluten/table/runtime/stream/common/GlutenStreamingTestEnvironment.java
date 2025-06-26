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
package org.apache.gluten.table.runtime.stream.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.loader.PlannerModule;
import org.apache.flink.table.planner.runtime.utils.StreamTestSink;
import org.apache.flink.test.junit5.MiniClusterExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

class GlutenStreamingTestEnvironement {

  private static final String EXECUTION_PLAN_PREIFX = "== Physical Execution Plan ==";

  private static final int DEFAULT_PARALLELISM = 4;
  protected static final Configuration GLUEN_CONF =
      new Configuration() {
        {
          set(RestOptions.PORT, 8181);
          set(TaskManagerOptions.NUM_TASK_SLOTS, DEFAULT_PARALLELISM);
        }
      };
  protected StreamExecutionEnvironment glutenEnv;
  protected StreamTableEnvironment glutenTEnv;

  protected static final Configuration FLINK_CONF =
      new Configuration() {
        {
          set(RestOptions.PORT, 8281);
          set(TaskManagerOptions.NUM_TASK_SLOTS, DEFAULT_PARALLELISM);
        }
      };
  protected StreamExecutionEnvironment flinkEnv;
  protected StreamTableEnvironment flinkTEnv;
  protected boolean enableObjectReuse = true;

  @RegisterExtension
  private static final MiniClusterExtension GLUTEN_CLUSTER =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setConfiguration(GLUEN_CONF)
              .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
              .build());

  @RegisterExtension
  private static final MiniClusterExtension FLINK_CLUSTER =
      new MiniClusterExtension(
          new MiniClusterResourceConfiguration.Builder()
              .setNumberTaskManagers(1)
              .setConfiguration(FLINK_CONF)
              .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
              .build());

  @BeforeAll
  public static void setup() throws Exception {
    Velox4jEnvironment.initializeOnce();
  }

  @BeforeEach
  public void before() throws Exception {
    PlannerModule.setEnableGluten(true);
    glutenEnv = StreamExecutionEnvironment.getExecutionEnvironment(GLUEN_CONF);
    if (enableObjectReuse) {
      glutenEnv.getConfig().enableObjectReuse();
    }
    glutenTEnv =
        StreamTableEnvironment.create(
            glutenEnv, EnvironmentSettings.newInstance().inStreamingMode().build());

    // The classes implemented internally within Gluten will not override those implemented in
    // Flink。
    // This will make the execution run under Flink's planner.
    PlannerModule.setEnableGluten(false);
    flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment(FLINK_CONF);
    if (enableObjectReuse) {
      flinkEnv.getConfig().enableObjectReuse();
    }
    flinkTEnv =
        StreamTableEnvironment.create(
            flinkEnv, EnvironmentSettings.newInstance().inStreamingMode().build());
  }

  @AfterEach
  public void after() {
    StreamTestSink.clear();
    TestValuesTableFactory.clearAllData();
  }
}
