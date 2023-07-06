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

import io.glutenproject.GlutenConfig
import io.glutenproject.utils.UTSystemParameters

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import java.nio.file.Files

import scala.collection.immutable.Seq

class GlutenFunctionValidateSuite extends WholeStageTransformerSuite {
  override protected val resourcePath: String = {
    "../../../../gluten-core/src/test/resources/tpch-data"
  }
  override protected val backend: String = "ch"
  override protected val fileFormat: String = "parquet"
  protected val rootPath: String = getClass.getResource("/").getPath
  protected val basePath: String = rootPath + "unit-tests-working-home"

  protected val tablesPath: String = basePath + "/tpch-data"
  protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  protected val queriesResults: String = rootPath + "queries-output"
  protected val warehouse: String = basePath + "/spark-warehouse"
  protected val metaStorePathAbsolute: String = basePath + "/meta"

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
      .set("spark.gluten.sql.columnar.columnartorow", "true")
      .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
      .set(GlutenConfig.GLUTEN_LIB_PATH, UTSystemParameters.getClickHouseLibPath())
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      .set("spark.gluten.sql.columnar.forceshuffledhashjoin", "true")
      .set("spark.sql.warehouse.dir", warehouse)
      .set("spark.shuffle.manager", "sort")
      .set("spark.io.compression.codec", "snappy")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.gluten.sql.columnar.backend.ch.use.v2", "false")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    val lfile = Files.createTempFile("", ".parquet").toFile
    lfile.deleteOnExit()
    parquetPath = lfile.getAbsolutePath

    val schema = StructType(
      Array(
        StructField("double_field1", DoubleType, true),
        StructField("int_field1", IntegerType, true),
        StructField("string_field1", StringType, true)
      ))
    val data = sparkContext.parallelize(
      Seq(
        Row(1.025, 1, "{\"a\":\"b\"}"),
        Row(1.035, 2, null),
        Row(1.045, 3, "{\"1a\":\"b\"}"),
        Row(1.011, 4, "{\"a 2\":\"b\"}"),
        Row(1.011, 5, "{\"a_2\":\"b\"}"),
        Row(1.011, 5, "{\"a\":\"b\", \"x\":{\"i\":1}}"),
        Row(1.011, 5, "{\"a\":\"b\", \"x\":{\"i\":2}}")
      ))
    val dfParquet = spark.createDataFrame(data, schema)
    dfParquet
      .coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .parquet(parquetPath)

    spark.catalog.createTable("json_test", parquetPath, fileFormat)
  }

  test("Test get_json_object 1") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.a') from json_test") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test get_json_object 2") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.1a') from json_test") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test get_json_object 3") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$.a_2') from json_test") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  ignore("Test get_json_object 4") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$[a]') from json_test") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test get_json_object 5") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$[\\\'a\\\']') from json_test") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test get_json_object 6") {
    runQueryAndCompare("SELECT get_json_object(string_field1, '$[\\\'a 2\\\']') from json_test") {
      checkOperatorMatch[ProjectExecTransformer]
    }
  }

  test("Test get_json_object 7") {
    runQueryAndCompare(
      "SELECT get_json_object(string_field1, '$..') from json_test",
      noFallBack = false) { _ => }
  }

  test("Test get_json_object 8") {
    runQueryAndCompare(
      "SELECT get_json_object(string_field1, '$..') from json_test",
      noFallBack = false) { _ => }
  }

  test("Test get_json_object 9") {
    runQueryAndCompare(
      "SELECT get_json_object(string_field1, '$.x[?(@.i == 1)]') from json_test",
      noFallBack = false) { _ => }
  }

  test("Test covar_samp") {
    runQueryAndCompare("SELECT covar_samp(double_field1, int_field1) from json_test") { _ => }
  }

  test("Test covar_pop") {
    runQueryAndCompare("SELECT covar_pop(double_field1, int_field1) from json_test") { _ => }
  }

  test("test 'function xxhash64'") {
    val df = runQueryAndCompare(
      "select xxhash64(id) from range(10)"
    )(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df, 10)
  }

  test("test 'function murmur3hash'") {
    val df = runQueryAndCompare(
      "select hash(id) from range(10)"
    )(checkOperatorMatch[ProjectExecTransformer])
    checkLengthAndPlan(df, 10)
  }
}
