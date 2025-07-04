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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

@Disabled("Gluten has not supported part of job run in native")
class ScalarFunctionsTest extends GlutenStreamingTestBase {

  @Override
  @BeforeEach
  public void before() throws Exception {
    super.before();
  }

  @Test
  void testAdd() {
    List<Row> rows = Arrays.asList(Row.of(1, 1L), Row.of(2, 2L), Row.of(3, 3L));
    createSimpleBoundedValuesTable("tblAdd", "a int, b bigint", rows);

    String query1 = "select a + b as x from tblAdd where a > 0";
    runAndCheck(query1, Arrays.asList("+I[2]", "+I[4]", "+I[6]"));

    String query2 = "select a + 1 as x from tblAdd where a > 0";
    runAndCheck(query2, Arrays.asList("+I[2]", "+I[3]", "+I[4]"));
  }

  @Test
  void testSubtract() {
    List<Row> rows = Arrays.asList(Row.of(1, 1L), Row.of(2, 2L), Row.of(3, 3L));
    createSimpleBoundedValuesTable("tblSub", "a int, b bigint", rows);
    String query1 = "select a - b as x from tblSub where a > 0";
    runAndCheck(query1, Arrays.asList("+I[0]", "+I[0]", "+I[0]"));

    String query2 = "select a - 1 as x from tblSub where a > 0";
    runAndCheck(query2, Arrays.asList("+I[0]", "+I[1]", "+I[2]"));
  }

  @Test
  void testMod() {
    List<Row> rows = Arrays.asList(Row.of(1, 100), Row.of(2, 3), Row.of(3, 5));
    createSimpleBoundedValuesTable("tblMod", "a int, d int", rows);
    String query1 = "select d % a as x from tblMod where a > 0";
    runAndCheck(query1, Arrays.asList("+I[0]", "+I[1]", "+I[2]"));

    String query2 = "select d % 3 as x from tblMod where a > 0";
    runAndCheck(query2, Arrays.asList("+I[1]", "+I[0]", "+I[2]"));
  }

  @Test
  void testLargerThen() {
    List<Row> rows =
        Arrays.asList(Row.of(1, 1L, "2", "1"), Row.of(2, 2L, "2", "2"), Row.of(3, 3L, "2", "1"));
    createSimpleBoundedValuesTable("tblLarger", "a int, b bigint, c string, d string", rows);
    String query1 = "select a > 1 as x from tblLarger where a > 0";
    runAndCheck(query1, Arrays.asList("+I[false]", "+I[true]", "+I[true]"));

    String query2 = "select b > 1 as x from tblLarger where a > 0";
    runAndCheck(query2, Arrays.asList("+I[false]", "+I[true]", "+I[true]"));

    String query3 = "select a > c as x from tblLarger where a > 0";
    runAndCheck(query3, Arrays.asList("+I[false]", "+I[false]", "+I[true]"));

    String query4 = "select c > d as x from tblLarger where a > 0";
    runAndCheck(query4, Arrays.asList("+I[true]", "+I[false]", "+I[true]"));
  }

  @Test
  void testLessThen() {
    List<Row> rows =
        Arrays.asList(Row.of(1, 1L, "2", "1"), Row.of(2, 2L, "2", "2"), Row.of(3, 3L, "2", "1"));
    createSimpleBoundedValuesTable("tblLess", "a int, b bigint, c string, d string", rows);

    String query1 = "select a < 2 as x from tblLess where a > 0";
    runAndCheck(query1, Arrays.asList("+I[true]", "+I[false]", "+I[false]"));

    String query2 = "select b < 2 as x from tblLess where a > 0";
    runAndCheck(query2, Arrays.asList("+I[true]", "+I[false]", "+I[false]"));

    String query3 = "select a < c as x from tblLess where a > 0";
    runAndCheck(query3, Arrays.asList("+I[true]", "+I[false]", "+I[false]"));

    String query4 = "select c < d as x from tblLess where a > 0";
    runAndCheck(query4, Arrays.asList("+I[false]", "+I[false]", "+I[false]"));

    String query5 = "select c > '123' from tblLess where a > 0";
    runAndCheck(query5, Arrays.asList("+I[true]", "+I[true]", "+I[true]"));
  }

  @Test
  void testEqual() {
    List<Row> rows =
        Arrays.asList(Row.of(1, 1L, "2", "1"), Row.of(2, 2L, "2", "2"), Row.of(3, 3L, "2", "1"));
    createSimpleBoundedValuesTable("tblEqual", "a int, b bigint, c string, d string", rows);

    String query1 = "select a = 1 as x from tblEqual where a > 0";
    runAndCheck(query1, Arrays.asList("+I[true]", "+I[false]", "+I[false]"));

    String query2 = "select b = 1 as x from tblEqual where a > 0";
    runAndCheck(query2, Arrays.asList("+I[true]", "+I[false]", "+I[false]"));

    String query3 = "select a, c, a = c as x from tblEqual where a > 0";
    runAndCheck(query3, Arrays.asList("+I[1, 2, false]", "+I[2, 2, true]", "+I[3, 2, false]"));

    String query4 = "select c = d as x from tblEqual where a > 0";
    runAndCheck(query4, Arrays.asList("+I[false]", "+I[true]", "+I[false]"));
  }

  @Test
  void testDecimalMultiply() {
    List<Row> rows = Arrays.asList(Row.of(1, 100L));
    createSimpleBoundedValuesTable("testTbl", "id int, price bigint", rows);
    String query = "select id, 0.908 * price as x from testTbl";
    runAndCheck(query, Arrays.asList("+I[1, 90.800]"));
  }
}
