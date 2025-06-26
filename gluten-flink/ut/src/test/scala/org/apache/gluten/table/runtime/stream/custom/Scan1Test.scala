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
package org.apache.gluten.table.runtime.stream.custom

import org.apache.gluten.table.runtime.stream.common.GlutenStreamingAbstractTestBase

import org.apache.flink.types.Row
import org.junit.jupiter.api.Test
import play.api.libs.json._;

class Scan1Test extends GlutenStreamingAbstractTestBase {

  def hasGlutenCalcNode(planString: String): Unit = {
    val plan = Json.parse(planString)
    val execNodes = (plan \ "nodes")
      .as[JsArray]
      .value
      .filter(node => (node \ "type").as[String] == "gluten-calc")
    assert(
      execNodes.size > 0,
      s"Expected at least one gluten-calc node, but found ${execNodes.size}")
  }

  @Test
  def testFilter(): Unit = {
    val rows = Seq(
      Row.of(Int.box(1), Long.box(1L), "1"),
      Row.of(Int.box(2), Long.box(2L), "2"),
      Row.of(Int.box(3), Long.box(3L), "3")
    )
    createSimpleBoundedValuesTable("mt", "a int, b bigint, c string", rows)
    val query = "select a, b as b, c, a > 2 from mt where a > 0"
    compareResultWithFlink(query, hasGlutenCalcNode)
    dropTable("mt")
  }

  @Test
  def testStruct(): Unit = {

    val rows = Seq(
      Row.of(Int.box(1), Row.of(Long.box(2L), "abc")),
      Row.of(Int.box(2), Row.of(Long.box(3L), "def")),
      Row.of(Int.box(3), Row.of(Long.box(4L), "ghi"))
    )
    createSimpleBoundedValuesTable("structTbl", "a int, b ROW<x bigint, y string>", rows)
    val query: String = "select a, b.x, b.y from structTbl where a > 0"
    compareResultWithFlink(query, hasGlutenCalcNode)
    dropTable("structTbl")
  }
}
