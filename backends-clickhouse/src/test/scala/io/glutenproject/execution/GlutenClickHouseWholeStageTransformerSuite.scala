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
package io.glutenproject.execution

abstract class GlutenClickHouseWholeStageTransformerSuite extends WholeStageTransformerSuite {

  val DBL_EPSILON = 2.2204460492503131e-16
  val DBL_RELAX_EPSILON: Double = Math.pow(10, -11)
  val FLT_EPSILON = 1.19209290e-07f
  def AlmostEqualsIsRel(expected: Double, actual: Double, EPSILON: Double = DBL_EPSILON): Unit = {
    val diff = Math.abs(expected - actual)
    val epsilon = EPSILON * Math.max(Math.abs(expected), Math.abs(actual))
    if (diff > epsilon) {
      fail(s"""
              |expected: $expected
              |actual:   $actual
              | abs(expected-expected) ~ epsilon = $diff ~ $epsilon
              |""".stripMargin)
    }
  }
}
