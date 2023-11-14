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

import io.glutenproject.{GlutenConfig, VERSION}

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.execution.ui.{GlutenSQLAppStatusStore, SparkListenerSQLExecutionStart}
import org.apache.spark.status.ElementTrackingStore

class GlutenFallbackSuite extends GlutenSQLTestsTrait {

  test("test fallback logging") {
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
      assert(
        testAppender.loggingEvents.exists(
          _.getMessage.getFormattedMessage.contains(
            "Validation failed for plan: Scan parquet default.t, " +
              "due to: columnar FileScan is not enabled in FileSourceScanExec")))
    }
  }

  test("test fallback event") {
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

      val id = runExecution("SELECT * FROM t1 JOIN t2")
      val execution = glutenStore.execution(id)
      // broadcast exchange and broadcast nested loop join
      assert(execution.get.numFallbackNodes == 2)
      assert(
        execution.get.fallbackNodeToReason.head._2
          .contains("Gluten does not touch it or does not support it"))
    }
  }
}
