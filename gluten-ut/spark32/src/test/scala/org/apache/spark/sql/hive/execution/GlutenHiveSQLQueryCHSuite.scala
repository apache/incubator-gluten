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

import org.apache.gluten.execution.{FileSourceScanExecTransformer, TransformSupport}

import org.apache.spark.{DebugFilesystem, SparkConf}
import org.apache.spark.internal.config
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{DataFrame, GlutenSQLTestsTrait, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.{HiveTableScanExecTransformer, HiveUtils}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}

import scala.reflect.ClassTag

class GlutenHiveSQLQueryCHSuite extends GlutenSQLTestsTrait {
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

  def checkOperatorMatch[T <: TransformSupport](df: DataFrame)(implicit tag: ClassTag[T]): Unit = {
    val executedPlan = getExecutedPlan(df)
    assert(executedPlan.exists(plan => plan.getClass == tag.runtimeClass))
  }

  override def sparkConf: SparkConf = {
    defaultSparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.gluten.sql.columnar.backend.lib", "ch")
      .set("spark.sql.storeAssignmentPolicy", "legacy")
      .set("spark.hadoop.fs.file.impl", classOf[DebugFilesystem].getName)
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
  }

  testGluten("5249: Reading csv may throw Unexpected empty column") {
    withSQLConf(
      "spark.gluten.sql.complexType.scan.fallback.enabled" -> "false"
    ) {
      sql("DROP TABLE IF EXISTS test_5249;")
      sql(
        "CREATE TABLE test_5249 (name STRING, uid STRING) " +
          "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' " +
          "STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' " +
          "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';")
      sql("INSERT INTO test_5249 VALUES('name_1', 'id_1');")
      val df = spark.sql(
        "SELECT name, uid, count(distinct uid) total_uid_num from test_5249 " +
          "group by name, uid with cube;")
      checkAnswer(
        df,
        Seq(
          Row("name_1", "id_1", 1),
          Row("name_1", null, 1),
          Row(null, "id_1", 1),
          Row(null, null, 1)))
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_5249"),
      ignoreIfNotExists = true,
      purge = false)
  }
}
