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
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class NexmarkTest {

  private static final Logger LOG = LoggerFactory.getLogger(NexmarkTest.class);

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
  void testNexmarkDDLGeneration() {
    String createDatagenTable = readSqlFromFile("nexmark/ddl_gen.sql");
    TableResult result = tEnv.executeSql(createDatagenTable);
    assertThat(result.getJobClient().isPresent()).isFalse();

    String describeQuery = "DESCRIBE datagen";
    TableResult describeResult = tEnv.executeSql(describeQuery);

    List<Row> schemaRows = collectResults(describeResult, -1);
    assertThat(schemaRows).isNotEmpty();

    for (Row row : schemaRows) {
      LOG.info("Schema: {}", row);
    }
  }

  @Test
  void testNexmarkQ0Query() {
    String createDatagenTable = readSqlFromFile("nexmark/bid.sql");
    tEnv.executeSql(createDatagenTable);

    String q0Query = readSqlFromFile("nexmark/q0.sql");

    TableResult q0Result = tEnv.executeSql(q0Query);
    List<Row> actualResults = collectResults(q0Result, 5);

    assertThat(actualResults).isNotEmpty();
    verifyQ0Results(actualResults);
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

  private List<Row> collectResults(TableResult result, int maxRows) {
    List<Row> rows = new ArrayList<>();
    try (CloseableIterator<Row> iterator = result.collect()) {
      int count = 0;
      while (iterator.hasNext() && (maxRows == -1 || count < maxRows)) {
        rows.add(iterator.next());
        count++;
      }
    } catch (Exception e) {
      LOG.error("Error collecting results", e);
    }
    return rows;
  }

  private void verifyQ0Results(List<Row> results) {
    for (Row row : results) {
      LOG.info("Q0 Result: {}", row);
      assertThat(row.getArity()).isEqualTo(5);
    }
  }
}
