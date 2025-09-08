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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

@Disabled("Need to apply for the new interface of gluten operator")
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
  void testSplitIndex() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, 1L, "http://testflink/a/b/c"),
            Row.of(2, 2L, "http://testflink/a1/b1/c1"),
            Row.of(3, 3L, "http://testflink/a2/b2/c2"));
    createSimpleBoundedValuesTable("tblSplitIndex", "a int, b bigint, c string", rows);
    String query1 = "select split_index(c, '/', 3) from tblSplitIndex";
    runAndCheck(query1, Arrays.asList("+I[testflink]", "+I[testflink]", "+I[testflink]"));
    String query2 = "select split_index(c, '//', 2) from tblSplitIndex";
    runAndCheck(
        query2,
        Arrays.asList("+I[testflink/a/b/c]", "+I[testflink/a1/b1/c1]", "+I[testflink/a2/b2/c2]"));
    // Add some corner case tests from `ScalarFunctionsTest`#testSplitIndex in flink.
    rows = Arrays.asList(Row.of(1, 1L, "AQIDBA=="), Row.of(1, 2L, null));
    createSimpleBoundedValuesTable("tblSplitIndexFlink", "a int, b bigint, c string", rows);
    String queryForInvalidIndex =
        "select split_index(c, 'I', 2), split_index(c, 'I', -1), split_index(c, 'I', cast(null as INT)) "
            + "from tblSplitIndexFlink where a = 1";
    String queryForNullDelimiter =
        "select split_index(c, cast(null as VARCHAR), 0) from tblSplitIndexFlink where a = 1";
    String queryForNumbericDelimiter =
        "select split_index(c, 73, 0), split_index(c, 256, 0) from tblSplitIndexFlink where a = 1";
    String queryForNullInput = "select split_index(c, 'I', 0) from tblSplitIndexFlink where a = 2";
    runAndCheck(queryForInvalidIndex, Arrays.asList("+I[null, null, null]"));
    runAndCheck(queryForNullDelimiter, Arrays.asList("+I[null]"));
    runAndCheck(queryForNumbericDelimiter, Arrays.asList("+I[AQ, null]"));
    runAndCheck(queryForNullInput, Arrays.asList("+I[null]"));
  }

  @Test
  void testDecimal() {
    List<Row> rows =
        Arrays.asList(
            Row.of(1, new BigDecimal("1.0"), new BigDecimal("1.0"), 2L, 1.0),
            Row.of(2, new BigDecimal("2.0"), new BigDecimal("2.0"), 3L, 3.0),
            Row.of(3, new BigDecimal("3.0"), new BigDecimal("3.0"), 4L, 4.0));
    createSimpleBoundedValuesTable(
        "tblDecimal", "a int, b decimal(11, 2), c decimal(10, 3), d bigint, e double", rows);
    String query = "select b + c as x from tblDecimal where a > 0";
    runAndCheck(query, Arrays.asList("+I[2.000]", "+I[4.000]", "+I[6.000]"));

    query = "select b + a as x from tblDecimal where a > 0";
    runAndCheck(query, Arrays.asList("+I[2.00]", "+I[4.00]", "+I[6.00]"));

    query = "select b + d as x from tblDecimal where a > 0";
    runAndCheck(query, Arrays.asList("+I[3.00]", "+I[5.00]", "+I[7.00]"));

    query = "select b - c as x from tblDecimal where a > 0";
    runAndCheck(query, Arrays.asList("+I[0.000]", "+I[0.000]", "+I[0.000]"));

    query = "select b - a as x from tblDecimal where a > 0";
    runAndCheck(query, Arrays.asList("+I[0.00]", "+I[0.00]", "+I[0.00]"));

    query = "select b * c as x from tblDecimal where a > 0";
    runAndCheck(query, Arrays.asList("+I[1.00000]", "+I[4.00000]", "+I[9.00000]"));

    query = "select b * d as x from tblDecimal where a > 0";
    runAndCheck(query, Arrays.asList("+I[2.00]", "+I[6.00]", "+I[12.00]"));

    query = "select b / c as x from tblDecimal where a > 0";
    runAndCheck(
        query, Arrays.asList("+I[1.0000000000000]", "+I[1.0000000000000]", "+I[1.0000000000000]"));

    query = "select b / a as x from tblDecimal where a > 0";
    runAndCheck(
        query, Arrays.asList("+I[1.0000000000000]", "+I[1.0000000000000]", "+I[1.0000000000000]"));

    query = "select b + e as x from tblDecimal where a > 0";
    runAndCheck(query, Arrays.asList("+I[2.0]", "+I[5.0]", "+I[7.0]"));
  }
}
