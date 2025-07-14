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
package org.apache.gluten.streaming.api.operators;

import org.apache.gluten.table.runtime.operators.GlutenVectorTwoInputOperator;

import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;

import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.gluten.streaming.api.operators.utils.RowDataTestUtils.checkEquals;

public class GlutenStreamJoinOperatorTest extends GlutenStreamJoinOperatorTestBase {

  @Test
  public void testInnerJoin() throws Exception {
    GlutenVectorTwoInputOperator operator = createJoinOperator(FlinkJoinType.INNER);
    TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, RowData> harness =
        createTestHarness(operator);

    processLeftTestData(harness, leftTestData);
    processRightTestData(harness, rightTestData);

    List<RowData> actualOutput = extractOutputFromHarness(harness);

    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(
                StringData.fromString("Ord#1"),
                StringData.fromString("LineOrd#2"),
                StringData.fromString("3 Bellevue Drive, Pottstown, PA 19464"),
                StringData.fromString("LineOrd#2"),
                StringData.fromString("AIR")),
            GenericRowData.of(
                StringData.fromString("Ord#2"),
                StringData.fromString("LineOrd#3"),
                StringData.fromString("68 Manor Station Street, Honolulu, HI 96815"),
                StringData.fromString("LineOrd#3"),
                StringData.fromString("TRUCK")));

    checkEquals(actualOutput, expectedOutput, outputRowType.getChildren());
    harness.close();
  }

  @Test
  @Disabled
  public void testLeftJoin() throws Exception {
    GlutenVectorTwoInputOperator operator = createJoinOperator(FlinkJoinType.LEFT);
    TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, RowData> harness =
        createTestHarness(operator);

    processLeftTestData(harness, leftTestData);
    processRightTestData(harness, rightTestData);

    List<RowData> actualOutput = extractOutputFromHarness(harness);

    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(
                StringData.fromString("Ord#1"),
                StringData.fromString("LineOrd#1"),
                StringData.fromString("3 Bellevue Drive, Pottstown, PA 19464"),
                null,
                null),
            GenericRowData.of(
                StringData.fromString("Ord#1"),
                StringData.fromString("LineOrd#2"),
                StringData.fromString("3 Bellevue Drive, Pottstown, PA 19464"),
                StringData.fromString("LineOrd#2"),
                StringData.fromString("AIR")),
            GenericRowData.of(
                StringData.fromString("Ord#2"),
                StringData.fromString("LineOrd#3"),
                StringData.fromString("68 Manor Station Street, Honolulu, HI 96815"),
                StringData.fromString("LineOrd#3"),
                StringData.fromString("TRUCK")));

    checkEquals(actualOutput, expectedOutput, outputRowType.getChildren());
    harness.close();
  }

  @Test
  @Disabled
  public void testRightJoin() throws Exception {
    GlutenVectorTwoInputOperator operator = createJoinOperator(FlinkJoinType.RIGHT);
    TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, RowData> harness =
        createTestHarness(operator);

    processLeftTestData(harness, leftTestData);
    processRightTestData(harness, rightTestData);

    List<RowData> actualOutput = extractOutputFromHarness(harness);

    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(
                StringData.fromString("Ord#1"),
                StringData.fromString("LineOrd#2"),
                StringData.fromString("3 Bellevue Drive, Pottstown, PA 19464"),
                StringData.fromString("LineOrd#2"),
                StringData.fromString("AIR")),
            GenericRowData.of(
                StringData.fromString("Ord#2"),
                StringData.fromString("LineOrd#3"),
                StringData.fromString("68 Manor Station Street, Honolulu, HI 96815"),
                StringData.fromString("LineOrd#3"),
                StringData.fromString("TRUCK")),
            GenericRowData.of(
                null,
                null,
                null,
                StringData.fromString("LineOrd#4"),
                StringData.fromString("SHIP")));

    checkEquals(actualOutput, expectedOutput, outputRowType.getChildren());
    harness.close();
  }

  @Test
  @Disabled
  public void testFullOuterJoin() throws Exception {
    GlutenVectorTwoInputOperator operator = createJoinOperator(FlinkJoinType.FULL);
    TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, RowData> harness =
        createTestHarness(operator);

    processLeftTestData(harness, leftTestData);
    processRightTestData(harness, rightTestData);

    List<RowData> actualOutput = extractOutputFromHarness(harness);

    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(
                StringData.fromString("Ord#1"),
                StringData.fromString("LineOrd#1"),
                StringData.fromString("3 Bellevue Drive, Pottstown, PA 19464"),
                null,
                null),
            GenericRowData.of(
                StringData.fromString("Ord#1"),
                StringData.fromString("LineOrd#2"),
                StringData.fromString("3 Bellevue Drive, Pottstown, PA 19464"),
                StringData.fromString("LineOrd#2"),
                StringData.fromString("AIR")),
            GenericRowData.of(
                StringData.fromString("Ord#2"),
                StringData.fromString("LineOrd#3"),
                StringData.fromString("68 Manor Station Street, Honolulu, HI 96815"),
                StringData.fromString("LineOrd#3"),
                StringData.fromString("TRUCK")),
            GenericRowData.of(
                null,
                null,
                null,
                StringData.fromString("LineOrd#4"),
                StringData.fromString("SHIP")));

    checkEquals(actualOutput, expectedOutput, outputRowType.getChildren());
    harness.close();
  }

  @Test
  public void testInnerJoinWithNonEquiCondition() throws Exception {
    GlutenVectorTwoInputOperator operator =
        createJoinOperator(FlinkJoinType.INNER, createNonEquiCondition());
    TwoInputStreamOperatorTestHarness<StatefulRecord, StatefulRecord, RowData> harness =
        createTestHarness(operator);

    processLeftTestData(harness, leftTestData);
    processRightTestData(harness, rightTestData);

    List<RowData> actualOutput = extractOutputFromHarness(harness);

    List<RowData> expectedOutput =
        Arrays.asList(
            GenericRowData.of(
                StringData.fromString("Ord#1"),
                StringData.fromString("LineOrd#2"),
                StringData.fromString("3 Bellevue Drive, Pottstown, PA 19464"),
                StringData.fromString("LineOrd#2"),
                StringData.fromString("AIR")));

    checkEquals(actualOutput, expectedOutput, outputRowType.getChildren());
    harness.close();
  }
}
