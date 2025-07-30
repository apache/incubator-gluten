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
package org.apache.gluten.table.planner.expressions

import org.apache.gluten.table.planner.expressions.utils.GlutenExpressionTestBase

import org.apache.flink.table.planner.expressions.DecimalTypeTest
import org.junit.jupiter.api.{AfterEach, BeforeEach, Disabled, Test}

class GlutenDecimalTypeTest extends DecimalTypeTest with GlutenExpressionTestBase {
  @BeforeEach
  override def prepare(): Unit = {
    GlutenHelper.before()
    setupGlutenTestTable()
    glutenTestCases.clear()
    glutenExceptionTests.clear()
  }

  @AfterEach
  override def evaluateExprs(): Unit = {
    evaluateGlutenTestCases()
    evaluateGlutenExceptionTests()
  }

  @Test
  @Disabled
  override def testExactionFunctions(): Unit = {}

  @Test
  @Disabled
  override def testComparison(): Unit = {}

  @Test
  @Disabled
  override def testMod(): Unit = {}

  @Test
  @Disabled
  override def testCompareDecimalColWithNull(): Unit = {
    // This test is disabled for Gluten
  }

  @Test
  @Disabled
  override def testDecimalComparison(): Unit = {}

  @Test
  @Disabled
  override def testDecimalArithmetic(): Unit = {}

  @Test
  @Disabled
  override def testCaseWhen(): Unit = {}

  @Test
  @Disabled
  override def testUnaryPlusMinus(): Unit = {}

  @Test
  @Disabled
  override def testEquality(): Unit = {}
}
