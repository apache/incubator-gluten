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

import org.apache.gluten.table.runtime.stream.common.GlutenStreamingTestBase;

import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

class ScanTest extends GlutenStreamingTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ScanTest.class);

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testFilter() {
    List<Row> rows = Arrays.asList(Row.of(1, 1L, "1"), Row.of(2, 2L, "2"), Row.of(3, 3L, "3"));
    createSimpleBoundedValuesTable("MyTable", "a int, b bigint, c string", rows);
    String query = "select a, b as b,c, a > 2 from MyTable where a > 0";
    LOG.info("execution plan: {}", explainExecutionPlan(query));
    runAndCheck(
        query, Arrays.asList("+I[1, 1, 1, false]", "+I[2, 2, 2, false]", "+I[3, 3, 3, true]"));
  }

  @Test
  void testStructScan() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, Row.of(2L, "abc")),
            Row.of(2, Row.of(6L, "def")),
            Row.of(3, Row.of(8L, "ghi")));
    createSimpleBoundedValuesTable("strctTbl", "a int, b ROW<x bigint, y string>", rows);
    String query1 = "select a, b.x, b.y from strctTbl where a > 0";
    runAndCheck(query1, Arrays.asList("+I[1, 2, abc]", "+I[2, 6, def]", "+I[3, 8, ghi]"));

    String query2 = "select a, b from strctTbl where a > 1";
    runAndCheck(query2, Arrays.asList("+I[2, +I[6, def]]", "+I[3, +I[8, ghi]]"));
  }

  @Test
  void testDoubleScan() {
    List<Row> rows = Arrays.asList(Row.of(1, 1.0), Row.of(2, 2.0));
    createSimpleBoundedValuesTable("floatTbl", "a int, b double", rows);
    String query = "select a, b from floatTbl where a > 0";
    runAndCheck(query, Arrays.asList("+I[1, 1.0]", "+I[2, 2.0]"));
  }
}
