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
package org.apache.spark.sql.execution

import org.apache.gluten.execution._

import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.delta.metric.IncrementMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.createMetric

class GlutenDeltaExpressionSuite extends ParquetSuite {

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "20000000")
      .set("spark.sql.storeAssignmentPolicy", "legacy")
      .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  }

  test("test project IncrementMetric not fallback") {
    val table_name = "project_increment_metric"
    withTable(table_name) {
      val s = createTableBuilder(table_name, "delta", s"$dataHome/$table_name")
        .withProps(Map("delta.enableDeletionVectors" -> "'true'"))
        .withTableKey("lineitem")
        .build()
      spark.sql(s)

      spark.sql(s"""insert into table $table_name select * from lineitem""".stripMargin)
      val metric = createMetric(sparkContext, "number of source rows")
      val metricFilter = createMetric(sparkContext, "number of source rows (during repeated scan)")
      val df = sql(s"select l_orderkey,l_shipdate from $table_name")
        .withColumn("im", Column(IncrementMetric(Literal(true), metric)))
        .filter("im")
        .filter(Column(IncrementMetric(Literal(true), metricFilter)))
        .drop("im")
      df.collect()

      val cnt = df.queryExecution.executedPlan.collect {
        case _: DeltaProjectExecTransformer => true
        case _: DeltaFilterExecTransformer => true
      }

      assertResult(2)(cnt.size)
      assertResult(600572)(metric.value)
      assertResult(600572)(metricFilter.value)
    }
  }
}
