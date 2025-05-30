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
package org.apache.gluten.streaming.api.operators.co;

import org.apache.flink.streaming.api.operators.co.IntervalJoinOperatorTest;
import org.apache.flink.util.FlinkException;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class GlutenIntervalJoinOperatorTest extends IntervalJoinOperatorTest {

  private final boolean lhsFasterThanRhs;

  public GlutenIntervalJoinOperatorTest(boolean lhsFasterThanRhs) {
    super(lhsFasterThanRhs);
    this.lhsFasterThanRhs = lhsFasterThanRhs;
  }

  static Stream<Object> parameters() {
    return Stream.of(new Object[] {true});
  }

  @ParameterizedTest
  @MethodSource("parameters")
  @Disabled
  public void testImplementationMirrorsCorrectly() throws Exception {}

  @ParameterizedTest
  @MethodSource("parameters")
  @Disabled
  public void testNegativeExclusiveAndNegativeExlusive() throws Exception {}

  @ParameterizedTest
  @MethodSource("parameters")
  @Disabled
  public void testNegativeExclusiveAndPositiveExlusive() throws Exception {}

  @ParameterizedTest
  @MethodSource("parameters")
  @Disabled
  public void testPositiveExclusiveAndPositiveExlusive() throws Exception {}

  @ParameterizedTest
  @MethodSource("parameters")
  @Disabled
  public void testStateCleanupNegativeInclusiveNegativeInclusive() throws Exception {}

  @ParameterizedTest
  @MethodSource("parameters")
  @Disabled
  public void testStateCleanupNegativePositiveNegativeExlusive() throws Exception {}

  @ParameterizedTest
  @MethodSource("parameters")
  @Disabled
  public void testStateCleanupPositiveInclusivePositiveInclusive() throws Exception {}

  @ParameterizedTest
  @MethodSource("parameters")
  @Disabled
  public void testStateCleanupPositiveExlusivePositiveExclusive() throws Exception {}

  @ParameterizedTest
  @MethodSource("parameters")
  @Disabled
  public void testRestoreFromSnapshot() throws Exception {}

  @ParameterizedTest
  @MethodSource("parameters")
  @Disabled
  public void testContextCorrectLeftTimestamp() throws Exception {}

  @ParameterizedTest
  @MethodSource("parameters")
  @Disabled
  public void testReturnsCorrectTimestamp() throws Exception {}

  @ParameterizedTest
  @MethodSource("parameters")
  @Disabled
  public void testContextCorrectRightTimestamp() throws Exception {}

  @ParameterizedTest
  @MethodSource("parameters")
  @Disabled
  public void testFailsWithNoTimestampsLeft() throws Exception {
    throw new FlinkException("");
  }

  @ParameterizedTest
  @MethodSource("parameters")
  @Disabled
  public void testFailsWithNoTimestampsRight() throws Exception {
    throw new FlinkException("");
  }

  @ParameterizedTest
  @MethodSource("parameters")
  @Disabled
  public void testDiscardsLateData() throws Exception {}

  @ParameterizedTest
  @MethodSource("parameters")
  @Disabled
  public void testLateData() throws Exception {}
}
