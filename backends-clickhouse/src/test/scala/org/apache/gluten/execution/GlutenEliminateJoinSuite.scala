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
package org.apache.gluten.execution

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, NullPropagation}
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseConfig
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import java.nio.file.Files

class GlutenEliminateJoinSuite extends GlutenClickHouseWholeStageTransformerSuite {

  protected val tablesPath: String = basePath + "/tpch-data"
  protected val tpchQueries: String =
    rootPath + "../../../../tools/gluten-it/common/src/main/resources/tpch-queries"
  protected val queriesResults: String = rootPath + "queries-output"

  private var parquetPath: String = _

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.files.minPartitionNum", "1")
      .set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseSparkCatalog")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set(ClickHouseConfig.CLICKHOUSE_WORKER_ID, "1")
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.sql.warehouse.dir", warehouse)
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.gluten.supported.scala.udfs", "compare_substrings:compare_substrings")
      .set(
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key,
        ConstantFolding.ruleName + "," + NullPropagation.ruleName)
  }

  def createTable(tableName: String, rows: Seq[Row], schema: StructType): Unit = {
    val file = Files.createTempFile(tableName, ".parquet").toFile
    file.deleteOnExit()
    val data = sparkContext.parallelize(rows)
    val dataframe = spark.createDataFrame(data, schema)
    dataframe.coalesce(1).write.format("parquet").mode("overwrite").parquet(file.getAbsolutePath)
    spark.catalog.createTable(tableName, file.getAbsolutePath, "parquet")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    val schema1 = StructType(
      Array(
        StructField("k1", IntegerType, true),
        StructField("k2", IntegerType, true),
        StructField("v1", IntegerType, true),
        StructField("v2", IntegerType, true),
        StructField("v3", IntegerType, true)
      )
    )

    val data1 = Seq(
      Row(1, 1, 1, 1, 1),
      Row(1, 1, null, 2, 1),
      Row(1, 2, 1, 1, null),
      Row(2, 1, 1, null, 1),
      Row(2, 2, 1, 1, 1),
      Row(2, 2, 1, null, 1),
      Row(2, 2, 2, null, 3),
      Row(2, 3, 0, null, 1),
      Row(2, 4, 1, 2, 3),
      Row(3, 1, 4, 5, 6),
      Row(4, 2, 7, 8, 9),
      Row(5, 3, 10, 11, 12)
    )
    createTable("t1", data1, schema1)
    createTable("t2", data1, schema1)
    createTable("t3", data1, schema1)
  }

  test("Eliminate two aggregates join") {
    val sql = """
        select t1.k1, t1.k2, t2.k1, t2.k2, s1, s2 from (
          select k1, k2, sum(v1) s1 from (
            select * from t1 where k1 != 1
          )group by k1, k2
        ) t1 left join (
          select k1, k2, count(v1) s2 from (
            select * from t2 where k1 != 3
          )group by k1, k2
        ) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2
        order by t1.k1, t1.k2, s1, s2
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.length == 0)
      })
  }

  test("Elimiate three aggreages join") {
    val sql = """
        select t1.k1, t1.k2, t2.k1, t3.k2, s1, s2, s3 from (
          select k1, k2, sum(v1) s1 from (
            select * from t1 where k1 != 1
          )group by k1, k2
        ) t1 left join (
          select k1, k2, count(v1) s2 from (
            select * from t2 where k1 != 3
          )group by k1, k2
        ) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2
        left join (
          select k1, k2, count(v2) s3 from (
            select * from t3 where k1 != 3
          )group by k1, k2
        ) t3 on t1.k1 = t3.k1 and t1.k2 = t3.k2
        order by t1.k1, t1.k2, s1, s2, s3
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.length == 0)
      })
  }

  test("Left one join uneliminable") {
    val sql = """
        select t1.k1, t1.k2, s1, s2 from (
          select * from t1 where k1 != 1
        ) t1 left join (
          select k1, k2, count(v1) s1 from (
            select * from t2 where k1 != 1
          )group by k1, k2
        ) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2
        left join (
          select k1, k2, count(v2) s2 from (
            select * from t3 where k1 != 3
          )group by k1, k2
        ) t3 on t1.k1 = t3.k1 and t1.k2 = t3.k2
        order by t1.k1, t1.k2, s1, s2
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.length == 1)
      })
  }

  test("Left two joins uneliminable") {
    val sql = """
        select t1.k1, t1.k2, s1, s2 from (
          select k1, k2, count(v1) s1 from (
            select * from t2 where k1 != 1
          )group by k1, k2
        ) t1 left join (
          select * from t1 where k1 != 1
        ) t2 on t1.k1 = t2.k1 and t1.k2 = t2.k2
        left join (
          select k1, k2, count(v2) s2 from (
            select * from t3 where k1 != 3
          )group by k1, k2
        ) t3 on t1.k1 = t3.k1 and t1.k2 = t3.k2
        order by t1.k1, t1.k2, s1, s2
    """.stripMargin
    compareResultsAgainstVanillaSpark(
      sql,
      true,
      {
        df =>
          val joins = df.queryExecution.executedPlan.collect {
            case join: ShuffledHashJoinExecTransformerBase => join
          }
          assert(joins.length == 2)
      })
  }
}
