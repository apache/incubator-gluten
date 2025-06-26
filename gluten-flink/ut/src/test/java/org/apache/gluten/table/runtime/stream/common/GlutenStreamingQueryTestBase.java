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
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.loader.PlannerModule;
import org.apache.flink.table.planner.runtime.utils.StreamTestSink;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class GlutenStreamingQueryTestBase {

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

  @Test
  void demo() {
    List<Row> rows = Arrays.asList(Row.of(1, 1.0), Row.of(2, 2.0));
    createSimpleBoundedValuesTable("floatTbl", "a int, b double", rows);
    String query = "select a, b from floatTbl where a > 0";
    // The result is "+I[1, 1.0]", "+I[2, 2.0]".
    compareResultWithFlink(query);
  }

  protected void createSimpleBoundedValuesTable(String tableName, String schema, List<Row> rows) {
    String myTableDataId = TestValuesTableFactory.registerData(rows);
    String table =
        "CREATE TABLE "
            + tableName
            + "(\n"
            + schema
            + "\n"
            + ") WITH (\n"
            + " 'connector' = 'values',\n"
            + " 'bounded' = 'true',\n"
            + String.format(" 'data-id' = '%s',\n", myTableDataId)
            + " 'nested-projection-supported' = 'true'\n"
            + ")";
    glutenTEnv.executeSql(table);
    flinkTEnv.executeSql(table);
  }

  protected String explainExecutionPlan(String query, boolean useGluten) {
    Table table;
    if (useGluten) {
      table = glutenTEnv.sqlQuery(query);
    } else {
      table = flinkTEnv.sqlQuery(query);
    }
    String plainPlans = table.explain(ExplainDetail.JSON_EXECUTION_PLAN);
    int index = plainPlans.indexOf(EXECUTION_PLAN_PREIFX);
    if (index != -1) {
      return plainPlans.substring(index + EXECUTION_PLAN_PREIFX.length());
    } else {
      return "";
    }
  }

  protected void compareResultWithFlink(String query) {
    System.err.println("Gluten Execution Plan:\n" + explainExecutionPlan(query, true));
    System.err.println("Flink Execution Plan:\n" + explainExecutionPlan(query, false));
    List<String> actual =
        CollectionUtil.iteratorToList(glutenTEnv.executeSql(query).collect()).stream()
            .map(Object::toString)
            .collect(Collectors.toList());
    List<String> expected =
        CollectionUtil.iteratorToList(flinkTEnv.executeSql(query).collect()).stream()
            .map(Object::toString)
            .collect(Collectors.toList());
    assertThat(actual).isEqualTo(expected);
  }
}
