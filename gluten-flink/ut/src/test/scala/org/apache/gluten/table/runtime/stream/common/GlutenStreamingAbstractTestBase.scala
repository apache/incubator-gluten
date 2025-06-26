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
package org.apache.gluten.table.runtime.stream.common

import org.apache.flink.table.api.ExplainDetail
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

class GlutenStreamingAbstractTestBase extends GlutenStreamingTestEnvironement {
  final private val EXECUTION_PLAN_PREIFX = "== Physical Execution Plan ==";

  // @Test
  // def demo(): Unit = {
  //   val rows = Seq(
  //     Row.of(Int.box(1), Double.box(1.0)),
  //     Row.of(Int.box(2), Double.box(2.0))
  //   )
  //   createSimpleBoundedValuesTable("floatTbl", "a int, b double", rows);
  //   val query: String = "select a, b from floatTbl where a > 0";
  //   // The result is "+I[1, 1.0]", "+I[2, 2.0]".
  //   val checkOperatorOffloaded = (planString: String) => {
  //     val plan = Json.parse(planString)
  //     val execNodes = (plan \ "nodes")
  //       .as[JsArray]
  //       .value
  //       .filter(node => (node \ "type").as[String] == "gluten-calc")
  //     assert(execNodes.size == 1, s"Expected one gluten-calc node, but found ${execNodes.size}")
  //   }
  //   compareResultWithFlink(query, checkOperatorOffloaded);
  // }

  def explainExecutionPlan(query: String, useGluten: Boolean): String = {
    val table = if (useGluten) {
      glutenTEnv.sqlQuery(query)
    } else {
      flinkTEnv.sqlQuery(query)
    }
    val plainPlans = table.explain(ExplainDetail.JSON_EXECUTION_PLAN)
    val index = plainPlans.indexOf(EXECUTION_PLAN_PREIFX);
    if (index != -1) {
      plainPlans.substring(index + EXECUTION_PLAN_PREIFX.length());
    } else {
      "";
    }
  }

  def createSimpleBoundedValuesTable(tableName: String, schema: String, rows: Seq[Row]): Unit = {
    val tableId = TestValuesTableFactory.registerData(rows.asJava)
    val createTableStmt =
      s"""
         |CREATE TABLE $tableName (
         |  $schema
         |) WITH (
         |  'connector' = 'values',
         |  'data-id' = '$tableId',
         |  'nested-projection-supported' = 'true'
         |)
       """.stripMargin
    glutenTEnv.executeSql(createTableStmt)
    flinkTEnv.executeSql(createTableStmt)
  }

  def dropTable(tableName: String): Unit = {
    glutenTEnv.executeSql(s"DROP TABLE IF EXISTS $tableName")
    flinkTEnv.executeSql(s"DROP TABLE IF EXISTS $tableName")
  }

  def compareResultWithFlink(query: String, planChecker: String => Unit = _ => ()): Unit = {
    val glutenPlan = explainExecutionPlan(query, useGluten = true)
    planChecker(glutenPlan)
    val glutenResult = glutenTEnv.executeSql(query).collect().asScala.toSeq
    val flinkResult = flinkTEnv.executeSql(query).collect().asScala.toSeq
    assert(glutenResult == flinkResult, s"Results do not match: $glutenResult != $flinkResult")
  }
}
