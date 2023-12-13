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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row

class VeloxDeltaSuite extends WholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  }

  // IdMapping is supported in Delta 2.2 (related to Spark3.3.1)
  testWithSpecifiedSparkVersion("column mapping mode = id", Some("3.3.1")) {
    spark.sql(s"""
                 |create table delta_cm1 (id int, name string) using delta
                 |tblproperties ("delta.columnMapping.mode"= "id")
                 |""".stripMargin)
    spark.sql(s"""
                 |insert into delta_cm1 values (1, "v1"), (2, "v2")
                 |""".stripMargin)
    val df1 = runQueryAndCompare("select * from delta_cm1") { _ => }
    checkLengthAndPlan(df1, 2)
    checkAnswer(df1, Row(1, "v1") :: Row(2, "v2") :: Nil)

    val df2 = runQueryAndCompare("select name from delta_cm1 where id = 2") { _ => }
    checkLengthAndPlan(df2, 1)
    checkAnswer(df2, Row("v2") :: Nil)
  }

  // NameMapping is supported in Delta 2.0 (related to Spark3.2.0)
  testWithSpecifiedSparkVersion("column mapping mode = name", Some("3.2.0")) {
    spark.sql(s"""
                 |create table delta_cm2 (id int, name string) using delta
                 |tblproperties ("delta.columnMapping.mode"= "name")
                 |""".stripMargin)
    spark.sql(s"""
                 |insert into delta_cm2 values (1, "v1"), (2, "v2")
                 |""".stripMargin)
    val df1 = runQueryAndCompare("select * from delta_cm2") { _ => }
    checkLengthAndPlan(df1, 2)
    checkAnswer(df1, Row(1, "v1") :: Row(2, "v2") :: Nil)

    val df2 = runQueryAndCompare("select name from delta_cm2 where id = 2") { _ => }
    checkLengthAndPlan(df2, 1)
    checkAnswer(df2, Row("v2") :: Nil)
  }

  test("delta: time travel") {
    spark.sql(s"""
                 |create table delta_tm (id int, name string) using delta
                 |""".stripMargin)
    spark.sql(s"""
                 |insert into delta_tm values (1, "v1"), (2, "v2")
                 |""".stripMargin)
    spark.sql(s"""
                 |insert into delta_tm values (3, "v3"), (4, "v4")
                 |""".stripMargin)
    val df1 = runQueryAndCompare("select * from delta_tm VERSION AS OF 1") { _ => }
    checkLengthAndPlan(df1, 2)
    checkAnswer(df1, Row(1, "v1") :: Row(2, "v2") :: Nil)
    val df2 = runQueryAndCompare("select * from delta_tm VERSION AS OF 2") { _ => }
    checkLengthAndPlan(df2, 4)
    checkAnswer(df2, Row(1, "v1") :: Row(2, "v2") :: Row(3, "v3") :: Row(4, "v4") :: Nil)
    val df3 = runQueryAndCompare("select name from delta_tm VERSION AS OF 2 where id = 2") { _ => }
    checkLengthAndPlan(df3, 1)
    checkAnswer(df3, Row("v2") :: Nil)
  }

  test("delta: partition filters") {
    spark.sql(s"""
                 |create table delta_pf (id int, name string) using delta partitioned by (name)
                 |""".stripMargin)
    spark.sql(s"""
                 |insert into delta_pf values (1, "v1"), (2, "v2"), (3, "v1"), (4, "v2")
                 |""".stripMargin)
    val df1 = runQueryAndCompare("select * from delta_pf where name = 'v1'") { _ => }
    val deltaScanTransformer = df1.queryExecution.executedPlan.collect {
      case f: FileSourceScanExecTransformer => f
    }.head
    // No data filters as only partition filters exist
    assert(deltaScanTransformer.filterExprs().size == 0)
    checkLengthAndPlan(df1, 2)
    checkAnswer(df1, Row(1, "v1") :: Row(3, "v1") :: Nil)
  }

  test("basic test with stats.skipping disabled") {
    withSQLConf("spark.databricks.delta.stats.skipping" -> "false") {
      spark.sql(s"""
                   |create table delta_test2 (id int, name string) using delta
                   |""".stripMargin)
      spark.sql(s"""
                   |insert into delta_test2 values (1, "v1"), (2, "v2")
                   |""".stripMargin)
      val df1 = runQueryAndCompare("select * from delta_test2") { _ => }
      checkLengthAndPlan(df1, 2)
      checkAnswer(df1, Row(1, "v1") :: Row(2, "v2") :: Nil)

      val df2 = runQueryAndCompare("select name from delta_test2 where id = 2") { _ => }
      checkLengthAndPlan(df2, 1)
      checkAnswer(df2, Row("v2") :: Nil)
    }
  }
}
