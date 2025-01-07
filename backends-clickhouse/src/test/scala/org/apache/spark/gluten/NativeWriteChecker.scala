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
package org.apache.spark.gluten

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.GlutenClickHouseWholeStageTransformerSuite

import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.{ColumnarWriteFilesExec, QueryExecution}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.FakeRowAdaptor
import org.apache.spark.sql.util.QueryExecutionListener

trait NativeWriteChecker
  extends GlutenClickHouseWholeStageTransformerSuite
  with AdaptiveSparkPlanHelper {

  private val formats: Seq[String] = Seq("orc", "parquet")

  def withNativeWriteCheck(checkNative: Boolean)(block: => Unit): Unit = {
    var nativeUsed = false

    val queryListener = new QueryExecutionListener {
      override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = {
        fail("query failed", e)
      }
      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        if (!nativeUsed) {
          val executedPlan = stripAQEPlan(qe.executedPlan)
          nativeUsed = if (isSparkVersionGE("3.5")) {
            executedPlan.find(_.isInstanceOf[ColumnarWriteFilesExec]).isDefined
          } else {
            executedPlan.find(_.isInstanceOf[FakeRowAdaptor]).isDefined
          }
        }
      }
    }
    try {
      spark.listenerManager.register(queryListener)
      block
      spark.sparkContext.listenerBus.waitUntilEmpty()
      assertResult(checkNative)(nativeUsed)
    } finally {
      spark.listenerManager.unregister(queryListener)
    }
  }
  def checkInsertQuery(sqlStr: String, checkNative: Boolean): Unit =
    withNativeWriteCheck(checkNative) {
      spark.sql(sqlStr)
    }

  def withDestinationTable(table: String, createTableSql: Option[String] = None)(
      f: => Unit): Unit = {
    spark.sql(s"drop table IF EXISTS $table")
    createTableSql.foreach(spark.sql)
    f
  }

  def nativeWrite(f: String => Unit): Unit = {
    withSQLConf((GlutenConfig.NATIVE_WRITER_ENABLED.key, "true")) {
      formats.foreach(f(_))
    }
  }

  def vanillaWrite(block: => Unit): Unit = {
    withSQLConf((GlutenConfig.NATIVE_WRITER_ENABLED.key, "false")) {
      block
    }
  }

  def withSource(df: Dataset[Row], viewName: String, pairs: (String, String)*)(
      block: => Unit): Unit = {
    withSQLConf(pairs: _*) {
      withTempView(viewName) {
        df.createOrReplaceTempView(viewName)
        block
      }
    }
  }
}
