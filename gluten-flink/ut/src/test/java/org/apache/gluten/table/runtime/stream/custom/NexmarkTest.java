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
    String createDatagenTable =
        "CREATE TABLE datagen ("
            + "  event_type INT,"
            + "  person ROW<id BIGINT, name STRING, dateTime TIMESTAMP(3)>,"
            + "  auction ROW<id BIGINT, itemName STRING, dateTime TIMESTAMP(3)>,"
            + "  bid ROW<auction BIGINT, bidder BIGINT, price BIGINT, dateTime TIMESTAMP(3), extra STRING>"
            + ") WITH ("
            + "  'connector' = 'datagen',"
            + "  'number-of-rows' = '10'"
            + ")";

    TableResult result = tEnv.executeSql(createDatagenTable);
    assertThat(result.getJobClient().isPresent()).isFalse();

    String createBidTable =
        "CREATE TABLE bid ("
            + "  auction BIGINT,"
            + "  bidder BIGINT,"
            + "  price BIGINT,"
            + "  dateTime TIMESTAMP(3),"
            + "  extra STRING,"
            + "  WATERMARK FOR dateTime AS dateTime - INTERVAL '4' SECOND"
            + ") WITH ("
            + "  'connector' = 'datagen',"
            + "  'number-of-rows' = '5'"
            + ")";

    TableResult bidResult = tEnv.executeSql(createBidTable);
    assertThat(bidResult.getJobClient().isPresent()).isFalse();

    String describeQuery = "DESCRIBE datagen";
    TableResult describeResult = tEnv.executeSql(describeQuery);

    List<Row> schemaRows = new ArrayList<>();
    try (CloseableIterator<Row> iterator = describeResult.collect()) {
      while (iterator.hasNext()) {
        schemaRows.add(iterator.next());
      }
    } catch (Exception e) {
      LOG.error("Error collecting schema info", e);
    }

    assertThat(schemaRows).hasSize(4);
  }

  @Test
  void testNexmarkQ0Query() {
    String createSourceTable =
        "CREATE TABLE bid_source ("
            + "  auction BIGINT,"
            + "  bidder BIGINT,"
            + "  price BIGINT,"
            + "  dateTime TIMESTAMP(3),"
            + "  extra STRING"
            + ") WITH ("
            + "  'connector' = 'datagen',"
            + "  'number-of-rows' = '3'"
            + ")";

    tEnv.executeSql(createSourceTable);

    String q0Query = "SELECT auction, bidder, price, dateTime, extra FROM bid_source";

    TableResult q0Result = tEnv.executeSql(q0Query);

    List<Row> actualResults = new ArrayList<>();
    try (CloseableIterator<Row> iterator = q0Result.collect()) {
      int count = 0;
      while (iterator.hasNext() && count < 3) {
        actualResults.add(iterator.next());
        count++;
      }
    } catch (Exception e) {
      LOG.error("Error collecting results", e);
    }

    assertThat(actualResults).isNotEmpty();

    for (Row row : actualResults) {
      LOG.info("Result row: {}", row);
    }
  }

  @Test
  void testWatermarkIntervalCalculation() {
    String createTableWithWatermark =
        "CREATE TABLE test_watermark ("
            + "  id BIGINT,"
            + "  event_time TIMESTAMP(3),"
            + "  WATERMARK FOR event_time AS event_time - INTERVAL '4' SECOND"
            + ") WITH ("
            + "  'connector' = 'datagen',"
            + "  'number-of-rows' = '3'"
            + ")";

    tEnv.executeSql(createTableWithWatermark);

    String intervalQuery = "SELECT id, event_time FROM test_watermark";

    TableResult intervalResult = tEnv.executeSql(intervalQuery);

    List<Row> intervalRows = new ArrayList<>();
    try (CloseableIterator<Row> iterator = intervalResult.collect()) {
      int count = 0;
      while (iterator.hasNext() && count < 3) {
        intervalRows.add(iterator.next());
        count++;
      }
    } catch (Exception e) {
      LOG.error("Error collecting interval results", e);
    }

    assertThat(intervalRows).isNotEmpty();
  }

  @Test
  void testCaseWhenWithRowTypes() {
    String createTestTable =
        "CREATE TABLE case_test ("
            + "  event_type INT,"
            + "  time1 TIMESTAMP(3),"
            + "  time2 TIMESTAMP(3)"
            + ") WITH ("
            + "  'connector' = 'datagen',"
            + "  'number-of-rows' = '3'"
            + ")";

    tEnv.executeSql(createTestTable);

    String caseQuery =
        "SELECT event_type, "
            + "CASE WHEN event_type = 0 THEN time1 ELSE time2 END as computed_time "
            + "FROM case_test";

    TableResult caseResult = tEnv.executeSql(caseQuery);

    List<Row> caseRows = new ArrayList<>();
    try (CloseableIterator<Row> iterator = caseResult.collect()) {
      int count = 0;
      while (iterator.hasNext() && count < 3) {
        caseRows.add(iterator.next());
        count++;
      }
    } catch (Exception e) {
      LOG.error("Error collecting case when results", e);
    }

    assertThat(caseRows).isNotEmpty();

    for (Row row : caseRows) {
      LOG.info("CASE WHEN result: {}", row);
    }
  }
}
