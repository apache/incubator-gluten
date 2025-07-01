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
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class NexmarkTest {

  private static final Logger LOG = LoggerFactory.getLogger(NexmarkTest.class);
  private static final String NEXMARK_RESOURCE_DIR = "nexmark";

  private static final Map<String, String> NEXMARK_VARIABLES =
      new HashMap<String, String>() {
        {
          put("TPS", "10");
          put("EVENTS_NUM", "100");
          put("PERSON_PROPORTION", "1");
          put("AUCTION_PROPORTION", "3");
          put("BID_PROPORTION", "46");
        }
      };

  private static StreamTableEnvironment tEnv;

  @BeforeAll
  static void setup() {
    LOG.info("NexmarkTest setup");
    Velox4jEnvironment.initializeOnce();

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    tEnv = StreamTableEnvironment.create(env, settings);

    setupNexmarkEnvironment(tEnv);
  }

  @Test
  void testAllNexmarkQueries() {
    List<String> queryFiles = getQueries();
    assertThat(queryFiles).isNotEmpty();

    LOG.info("Found {} Nexmark query files: {}", queryFiles.size(), queryFiles);

    for (String queryFile : queryFiles) {
      LOG.info("Executing query from file: {}", queryFile);
      executeQuery(tEnv, queryFile);
    }
  }

  private static void setupNexmarkEnvironment(StreamTableEnvironment tEnv) {
    String createNexmarkSource = readSqlFromFile(NEXMARK_RESOURCE_DIR + "/ddl_gen.sql");
    createNexmarkSource = replaceVariables(createNexmarkSource, NEXMARK_VARIABLES);
    tEnv.executeSql(createNexmarkSource);

    String createTableView = readSqlFromFile(NEXMARK_RESOURCE_DIR + "/ddl_views.sql");
    String[] sqlTableView = createTableView.split(";");
    for (String sql : sqlTableView) {
      String trimmedSql = sql.trim();
      if (!trimmedSql.isEmpty()) {
        tEnv.executeSql(trimmedSql);
      }
    }
  }

  private static String replaceVariables(String sql, Map<String, String> variables) {
    String result = sql;
    for (Map.Entry<String, String> entry : variables.entrySet()) {
      result = result.replace("${" + entry.getKey() + "}", entry.getValue());
    }
    return result;
  }

  private void executeQuery(StreamTableEnvironment tEnv, String queryFileName) {
    String queryContent = readSqlFromFile(NEXMARK_RESOURCE_DIR + "/" + queryFileName);

    String[] sqlStatements = queryContent.split(";");
    assertThat(sqlStatements.length).isGreaterThanOrEqualTo(2);

    String createResultTable = sqlStatements[0].trim();
    if (!createResultTable.isEmpty()) {
      TableResult createResult = tEnv.executeSql(createResultTable);
      assertThat(createResult.getJobClient().isPresent()).isFalse();
    }

    String insertQuery = sqlStatements[1].trim();
    if (!insertQuery.isEmpty()) {
      TableResult insertResult = tEnv.executeSql(insertQuery);
      assertThat(insertResult.getJobClient().isPresent()).isTrue();
      try {
        waitForJobCompletion(insertResult, 30000);
      } catch (Exception e) {
        throw new RuntimeException("Query execution failed: " + queryFileName, e);
      }
    }
  }

  private void waitForJobCompletion(TableResult result, long timeoutMs) {
    if (result.getJobClient().isPresent()) {
      var jobClint = result.getJobClient().get();
      try {
        jobClint.getJobExecutionResult().get(timeoutMs, TimeUnit.MILLISECONDS);
      } catch (ExecutionException e) {
        throw new RuntimeException("Job timeout after ", e);
      } catch (TimeoutException e) {
        throw new RuntimeException("Job execution failed ", e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private List<String> getQueries() {
    URL resourceUrl = getClass().getClassLoader().getResource(NEXMARK_RESOURCE_DIR);

    try {
      Path resourcePath = Paths.get(resourceUrl.toURI());
      List<String> queryFiles = new ArrayList<>();

      try (DirectoryStream<Path> stream = Files.newDirectoryStream(resourcePath, "q*.sql")) {
        for (Path entry : stream) {
          queryFiles.add(entry.getFileName().toString());
        }
      }

      return queryFiles.stream().sorted().collect(Collectors.toList());

    } catch (URISyntaxException | IOException e) {
      throw new RuntimeException("Failed to discover query files", e);
    }
  }

  private static String readSqlFromFile(String fileName) {
    try {
      URL resource = NexmarkTest.class.getClassLoader().getResource(fileName);
      if (resource == null) {
        throw new RuntimeException("SQL file not found: " + fileName);
      }
      return new String(Files.readAllBytes(Paths.get(resource.toURI())));
    } catch (Exception e) {
      throw new RuntimeException("Failed to read SQL file: " + fileName, e);
    }
  }
}
