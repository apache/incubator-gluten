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
package org.apache.spark.sql.hive.execution

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.HiveTableScanExecTransformer
import org.apache.spark.sql.internal.SQLConf

class GlutenHiveSQLQuerySuite extends GlutenHiveSQLQuerySuiteBase {

  override def sparkConf: SparkConf = {
    defaultSparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
  }

  testGluten("hive orc scan") {
    withSQLConf("spark.sql.hive.convertMetastoreOrc" -> "false") {
      sql("DROP TABLE IF EXISTS test_orc")
      sql(
        "CREATE TABLE test_orc (name STRING, favorite_color STRING)" +
          " USING hive OPTIONS(fileFormat 'orc')")
      sql("INSERT INTO test_orc VALUES('test_1', 'red')");
      val df = spark.sql("select * from test_orc")
      checkAnswer(df, Seq(Row("test_1", "red")))
      checkOperatorMatch[HiveTableScanExecTransformer](df)
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_orc"),
      ignoreIfNotExists = true,
      purge = false)
  }

  testGluten("test bloom filter join on partition column") {
    val tables = Seq("test_tbl1", "test_tbl2")
    tables.foreach {
      t =>
        sql(s"DROP TABLE IF EXISTS $t")
        sql(
          s"CREATE TABLE $t (a int) partitioned by (date STRING) " +
            s"stored as parquet")
    }

    withSQLConf(
      SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED.key -> "false",
      SQLConf.RUNTIME_BLOOM_FILTER_ENABLED.key -> "true",
      SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key -> "1",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1"
    ) {
      sql(
        "INSERT OVERWRITE TABLE test_tbl1 partition (date='20240101') " +
          "select id from range(10)");
      sql(
        "INSERT OVERWRITE TABLE test_tbl2 partition (date='20240101') " +
          "select id from range(5, 10)");

      val df = spark.sql(
        "select count(1) from test_tbl1 join test_tbl2 " +
          "on test_tbl1.date = test_tbl2.date where test_tbl2.a > 7")
      checkAnswer(df, Seq(Row(20)))
    }

    tables.foreach {
      t =>
        spark.sessionState.catalog.dropTable(
          TableIdentifier(t),
          ignoreIfNotExists = true,
          purge = false)
    }
  }

}
