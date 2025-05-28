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

import java.util.Arrays;
import java.util.List;

class ScalarFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
    List<Row> rows =
        Arrays.asList(
            Row.of(1, 1L, "1", 100, 10L), Row.of(2, 2L, "2", 3, 11L), Row.of(3, 3L, "3", 5, 12L));
    createSimpleBoundedValuesTable("t1", "a int, b bigint, c string, d int, e bigint", rows);

    List<Row> rows2 =
        Arrays.asList(Row.of(1, 2L, 3.0, "12"), Row.of(2, 3L, 4.0, "13"), Row.of(3, 4L, 5.0, "14"));
    createSimpleBoundedValuesTable("t2", "a int, b bigint, c double, d string", rows2);
  }

  @Test
  void testAdd() {
    String query1 = "select a + b as x from t1 where a > 0";
    runAndCheck(query1, Arrays.asList("+I[2]", "+I[4]", "+I[6]"));

    String query2 = "select a + 1 as x from t1 where a > 0";
    runAndCheck(query2, Arrays.asList("+I[2]", "+I[3]", "+I[4]"));
  }

  @Test
  void testSubtract() {
    String query1 = "select a - b as x from t1 where a > 0";
    runAndCheck(query1, Arrays.asList("+I[0]", "+I[0]", "+I[0]"));

    String query2 = "select a - 1 as x from t1 where a > 0";
    runAndCheck(query2, Arrays.asList("+I[0]", "+I[1]", "+I[2]"));
  }

  @Test
  void testMod() {
    String query1 = "select d % a as x from t1 where a > 0";
    runAndCheck(query1, Arrays.asList("+I[0]", "+I[1]", "+I[2]"));

    String query2 = "select d % 3 as x from t1 where a > 0";
    runAndCheck(query2, Arrays.asList("+I[1]", "+I[0]", "+I[2]"));
  }

  @Test
  void testLargerThen() {
    String query1 = "select a > 1 as x from t1 where a > 0";
    runAndCheck(query1, Arrays.asList("+I[false]", "+I[true]", "+I[true]"));

    String query2 = "select b > 1 as x from t1 where a > 0";
    runAndCheck(query2, Arrays.asList("+I[false]", "+I[true]", "+I[true]"));
  }

  @Test
  void testLessThen() {
    String query1 = "select a < 2 as x from t1 where a > 0";
    runAndCheck(query1, Arrays.asList("+I[true]", "+I[false]", "+I[false]"));

    String query2 = "select b < 2 as x from t1 where a > 0";
    runAndCheck(query2, Arrays.asList("+I[true]", "+I[false]", "+I[false]"));
  }

  @Test
  void testEqual() {
    String query1 = "select a = 1 as x from t1 where a > 0";
    runAndCheck(query1, Arrays.asList("+I[true]", "+I[false]", "+I[false]"));

    String query2 = "select b = 1 as x from t1 where a > 0";
    runAndCheck(query2, Arrays.asList("+I[true]", "+I[false]", "+I[false]"));
  }
}
