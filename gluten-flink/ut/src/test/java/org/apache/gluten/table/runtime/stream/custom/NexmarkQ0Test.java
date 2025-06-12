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
package org.apache.gluten.table.runtime.stream.custom;

import org.apache.gluten.table.runtime.stream.common.Velox4jEnvironment;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

public class NexmarkQ0Test {

  private static final Logger LOG = LoggerFactory.getLogger(NexmarkQ0Test.class);

  private StreamExecutionEnvironment env;
  private StreamTableEnvironment tEnv;

  @BeforeAll
  public static void setupAll() {
    LOG.info("NexmarkTest setup");
    Velox4jEnvironment.initializeOnce();
  }

  @BeforeEach
  public void setup() {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    tEnv = StreamTableEnvironment.create(env, settings);
  }

  @Test
  void testNexmarkQ0Query() {
    String createNexmarkSource = readSqlFromFile("nexmark/ddl_gen.sql");
    tEnv.executeSql(createNexmarkSource);
    String createTableView = readSqlFromFile("nexmark/ddl_views.sql");
    String[] sqlTableView = createTableView.split(";");
    for (String sql : sqlTableView) {
      String trimmedSql = sql.trim();
      tEnv.executeSql(trimmedSql);
    }

    String q0Query = readSqlFromFile("nexmark/q0.sql");

    String[] sqlStatements = q0Query.split(";");
    String createResultTable = sqlStatements[0].trim();
    TableResult createResult = tEnv.executeSql(createResultTable);
    assertThat(createResult.getJobClient().isPresent()).isFalse();
    String insertQuery = sqlStatements[1].trim();
    TableResult insertResult = tEnv.executeSql(insertQuery);
    assertThat(insertResult.getJobClient().isPresent()).isTrue();
  }

  private String readSqlFromFile(String fileName) {
    try {
      URL resource = getClass().getClassLoader().getResource(fileName);
      if (resource == null) {
        throw new RuntimeException("SQL file not found: " + fileName);
      }
      return new String(Files.readAllBytes(Paths.get(resource.toURI())));
    } catch (Exception e) {
      throw new RuntimeException("Failed to read SQL file: " + fileName, e);
    }
  }
}
