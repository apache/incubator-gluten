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

import io.glutenproject.execution.TransformSupport

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{DataFrame, GlutenSQLTestsTrait, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.hive.{HiveTableScanExecTransformer, HiveUtils}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

import scala.reflect.ClassTag

class GlutenHiveSQLQuerySuite extends GlutenSQLTestsTrait {
  private var _spark: SparkSession = null

  override def beforeAll(): Unit = {
    prepareWorkDir()
    if (_spark == null) {
      _spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    }

    _spark.sparkContext.setLogLevel("info")
  }

  override protected def spark: SparkSession = _spark

  override def afterAll(): Unit = {
    try {
      super.afterAll()
      if (_spark != null) {
        try {
          _spark.sessionState.catalog.reset()
        } finally {
          _spark.stop()
          _spark = null
        }
      }
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      doThreadPostAudit()
    }
  }

  protected def defaultSparkConf: SparkConf = {
    val conf = new SparkConf()
      .set("spark.master", "local[1]")
      .set("spark.sql.test", "")
      .set("spark.sql.testkey", "true")
      .set(SQLConf.CODEGEN_FALLBACK.key, "false")
      .set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.CODEGEN_ONLY.toString)
      .set(
        HiveUtils.HIVE_METASTORE_BARRIER_PREFIXES.key,
        "org.apache.spark.sql.hive.execution.PairSerDe")
      // SPARK-8910
      .set(UI_ENABLED, false)
      .set(config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
      // Hive changed the default of hive.metastore.disallow.incompatible.col.type.changes
      // from false to true. For details, see the JIRA HIVE-12320 and HIVE-17764.
      .set("spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes", "false")
      // Disable ConvertToLocalRelation for better test coverage. Test cases built on
      // LocalRelation will exercise the optimization rules better by disabling it as
      // this rule may potentially block testing of other optimization rules such as
      // ConstantPropagation etc.
      .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName)

    conf.set(
      StaticSQLConf.WAREHOUSE_PATH,
      conf.get(StaticSQLConf.WAREHOUSE_PATH) + "/" + getClass.getCanonicalName)
  }

  /**
   * Get all the children plan of plans.
   *
   * @param plans
   *   : the input plans.
   * @return
   */

  def checkOperatorMatch[T <: TransformSupport](df: DataFrame)(implicit tag: ClassTag[T]): Unit = {
    val executedPlan = getExecutedPlan(df)
    assert(executedPlan.exists(plan => plan.getClass == tag.runtimeClass))
  }

  override def sparkConf: SparkConf = {
    defaultSparkConf
      .set("spark.plugins", "io.glutenproject.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.gluten.sql.complexType.scan.fallback.enabled", "false")
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

  testGluten("avoid unnecessary filter binding for subfield during scan") {
    withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "false") {
      sql("DROP TABLE IF EXISTS test_subfield")
      sql(
        "CREATE TABLE test_subfield (name STRING, favorite_color STRING, " +
          " label STRUCT<label_1:STRING, label_2:STRING>) USING hive OPTIONS(fileFormat 'parquet')")
      sql(
        "INSERT INTO test_subfield VALUES('test_1', 'red', named_struct('label_1', 'label-a'," +
          " 'label_2', 'label-b'))");
      val df = spark.sql("select * from test_subfield where name='test_1'")
      checkAnswer(df, Seq(Row("test_1", "red", Row("label-a", "label-b"))))
      checkOperatorMatch[HiveTableScanExecTransformer](df)
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_subfield"),
      ignoreIfNotExists = true,
      purge = false)
  }

  testGluten("5128: Fix failed to parse post join filters") {
    withSQLConf(
      "spark.sql.hive.convertMetastoreParquet" -> "false",
      "spark.gluten.sql.complexType.scan.fallback.enabled" -> "false") {
      sql("DROP TABLE IF EXISTS test_5128_0;")
      sql("DROP TABLE IF EXISTS test_5128_1;")
      sql(
        "CREATE TABLE test_5128_0 (from_uid STRING, vgift_typeid int, vm_count int, " +
          "status bigint, ts bigint, vm_typeid int) " +
          "USING hive OPTIONS(fileFormat 'parquet') PARTITIONED BY (`day` STRING);")
      sql(
        "CREATE TABLE test_5128_1 (typeid int, groupid int, ss_id bigint, " +
          "ss_start_time bigint, ss_end_time bigint) " +
          "USING hive OPTIONS(fileFormat 'parquet');")
      sql(
        "INSERT INTO test_5128_0 partition(day='2024-03-31') " +
          "VALUES('uid_1', 2, 10, 1, 11111111111, 2);")
      sql("INSERT INTO test_5128_1 VALUES(2, 1, 1, 1000000000, 2111111111);")
      val df = spark.sql(
        "select ee.from_uid as uid,day, vgift_typeid, money from " +
          "(select t_a.day, if(cast(substr(t_a.ts,1,10) as bigint) between " +
          "t_b.ss_start_time and t_b.ss_end_time, t_b.ss_id, 0) ss_id, " +
          "t_a.vgift_typeid, t_a.from_uid, vm_count money from " +
          "(select from_uid,day,vgift_typeid,vm_count,ts from test_5128_0" +
          "where day between '2024-03-30' and '2024-03-31' and status=1 and vm_typeid=2) t_a " +
          "left join test_5128_1 t_b on t_a.vgift_typeid=t_b.typeid " +
          "where t_b.groupid in (1,2)) ee where ss_id=1;")
      checkAnswer(df, Seq(Row("uid_1", "2024-03-31", 2, 10)))
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_5128_0"),
      ignoreIfNotExists = true,
      purge = false)
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_5128_1"),
      ignoreIfNotExists = true,
      purge = false)
  }
}
