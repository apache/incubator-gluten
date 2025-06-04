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

import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class GlutenStreamingTestBase extends StreamingTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(GlutenStreamingTestBase.class);
  private static final String EXECUTION_PLAN_PREIFX = "== Physical Execution Plan ==";

  @BeforeAll
  public static void setup() throws Exception {
    LOG.info("GlutenStreamingTestBase setup");
    Velox4jEnvironment.initializeOnce();
  }

  /*
   * schema is in format of "a int, b bigint, c string"
   */
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
    tEnv().executeSql(table);
  }

  // Return the execution plan represented by StreamEexcNode
  protected String explainExecutionPlan(String query) {
    Table table = tEnv().sqlQuery(query);
    String plainPlans = table.explain(ExplainDetail.JSON_EXECUTION_PLAN);
    int index = plainPlans.indexOf(EXECUTION_PLAN_PREIFX);
    if (index != -1) {
      return plainPlans.substring(index + EXECUTION_PLAN_PREIFX.length());
    } else {
      return "";
    }
  }

  protected void runAndCheck(String query, List<String> expected) {
    List<String> actual =
        CollectionUtil.iteratorToList(tEnv().executeSql(query).collect()).stream()
            .map(Object::toString)
            .collect(Collectors.toList());
    assertThat(actual).isEqualTo(expected);
  }
}
