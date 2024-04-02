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
package org.apache.spark.sql.gluten

import org.apache.gluten.{GlutenConfig, VERSION}
import org.apache.gluten.events.GlutenPlanFallbackEvent
import org.apache.gluten.execution.FileSourceScanExecTransformer
import org.apache.gluten.utils.BackendTestUtils

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.{GlutenSQLTestsTrait, Row}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.ui.{GlutenSQLAppStatusStore, SparkListenerSQLExecutionStart}
import org.apache.spark.status.ElementTrackingStore

import scala.collection.mutable.ArrayBuffer

class GlutenFallbackSuite extends GlutenSQLTestsTrait with AdaptiveSparkPlanHelper {

  testGluten("test fallback logging") {
    val testAppender = new LogAppender("fallback reason")
    withLogAppender(testAppender) {
      withSQLConf(
        GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false",
        GlutenConfig.VALIDATION_LOG_LEVEL.key -> "error") {
        withTable("t") {
          spark.range(10).write.format("parquet").saveAsTable("t")
          sql("SELECT * FROM t").collect()
        }
      }
      val msgRegex = """Validation failed for plan: Scan parquet default\.t\[QueryId=[0-9]+\],""" +
        """ due to: columnar FileScan is not enabled in FileSourceScanExec\."""
      assert(testAppender.loggingEvents.exists(_.getMessage.getFormattedMessage.matches(msgRegex)))
    }
  }

  testGluten("test fallback event") {
    val kvStore = spark.sparkContext.statusStore.store.asInstanceOf[ElementTrackingStore]
    val glutenStore = new GlutenSQLAppStatusStore(kvStore)
    assert(glutenStore.buildInfo().info.find(_._1 == "Gluten Version").exists(_._2 == VERSION))

    def runExecution(sqlString: String): Long = {
      var id = 0L
      val listener = new SparkListener {
        override def onOtherEvent(event: SparkListenerEvent): Unit = {
          event match {
            case e: SparkListenerSQLExecutionStart => id = e.executionId
            case _ =>
          }
        }
      }
      spark.sparkContext.addSparkListener(listener)
      try {
        sql(sqlString).collect()
        spark.sparkContext.listenerBus.waitUntilEmpty()
      } finally {
        spark.sparkContext.removeSparkListener(listener)
      }
      id
    }

    withTable("t") {
      spark.range(10).write.format("parquet").saveAsTable("t")
      val id = runExecution("SELECT * FROM t")
      val execution = glutenStore.execution(id)
      assert(execution.isDefined)
      assert(execution.get.numGlutenNodes == 1)
      assert(execution.get.numFallbackNodes == 0)
      assert(execution.get.fallbackNodeToReason.isEmpty)

      withSQLConf(GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "false") {
        val id = runExecution("SELECT * FROM t")
        val execution = glutenStore.execution(id)
        assert(execution.isDefined)
        assert(execution.get.numGlutenNodes == 0)
        assert(execution.get.numFallbackNodes == 1)
        val fallbackReason = execution.get.fallbackNodeToReason.head
        assert(fallbackReason._1.contains("Scan parquet default.t"))
        assert(fallbackReason._2.contains("columnar FileScan is not enabled in FileSourceScanExec"))
      }
    }

    withTable("t1", "t2") {
      spark.range(10).write.format("parquet").saveAsTable("t1")
      spark.range(10).write.format("parquet").saveAsTable("t2")

      val id = runExecution("SELECT * FROM t1 FULL OUTER JOIN t2")
      val execution = glutenStore.execution(id)
      if (BackendTestUtils.isVeloxBackendLoaded()) {
        assert(execution.get.numFallbackNodes == 1)
        assert(
          execution.get.fallbackNodeToReason.head._2
            .contains("FullOuter join is not supported with BroadcastNestedLoopJoin"))
      } else {
        assert(execution.get.numFallbackNodes == 2)
      }
    }

    // [GLUTEN-4119] Skip add ReusedExchange to fallback node
    withTable("t1") {
      spark.range(10).write.format("parquet").saveAsTable("t1")
      val sql =
        "WITH sub1 AS (SELECT * FROM t1), sub2 AS (SELECT * FROM t1) SELECT * FROM sub1 JOIN sub2;"
      val id = runExecution(sql)
      val execution = glutenStore.execution(id)
      assert(!execution.get.fallbackNodeToReason.exists(_._1.contains("ReusedExchange")))
    }
  }

  testGluten("Improve merge fallback reason") {
    spark.sql("create table t using parquet as select 1 as c1, timestamp '2023-01-01' as c2")
    withTable("t") {
      val events = new ArrayBuffer[GlutenPlanFallbackEvent]
      val listener = new SparkListener {
        override def onOtherEvent(event: SparkListenerEvent): Unit = {
          event match {
            case e: GlutenPlanFallbackEvent => events.append(e)
            case _ =>
          }
        }
      }
      spark.sparkContext.addSparkListener(listener)
      withSQLConf(GlutenConfig.COLUMNAR_WHOLESTAGE_FALLBACK_THRESHOLD.key -> "1") {
        try {
          val df =
            spark.sql("select c1, count(*) from t where c2 > timestamp '2022-01-01' group by c1")
          checkAnswer(df, Row(1, 1))
          spark.sparkContext.listenerBus.waitUntilEmpty()

          // avoid failing when we support transform timestamp filter in future
          val isFallback = find(df.queryExecution.executedPlan) {
            _.isInstanceOf[FileSourceScanExecTransformer]
          }.isEmpty
          if (isFallback) {
            events.exists(
              _.fallbackNodeToReason.values.exists(
                _.contains("Subfield filters creation not supported for input type 'TIMESTAMP'")))
            events.exists(
              _.fallbackNodeToReason.values.exists(
                _.contains("Timestamp is not fully supported in Filter")))
          }
        } finally {
          spark.sparkContext.removeSparkListener(listener)
        }
      }
    }
  }
}
