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

import org.apache.spark.SparkConf
import org.apache.spark.internal.config
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.sql.{GlutenQueryTest, Row, SparkSession}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.execution.datasources.FakeRowAdaptor
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.util.Utils

import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile

import java.io.File

class VeloxParquetWriteForHiveSuite
  extends GlutenQueryTest
  with SQLTestUtils
  with BucketWriteUtils {
  private var _spark: SparkSession = _
  import testImplicits._

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    if (_spark == null) {
      _spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    }

    _spark.sparkContext.setLogLevel("info")
  }

  override protected def spark: SparkSession = _spark

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

  protected def sparkConf: SparkConf = {
    defaultSparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.gluten.sql.native.writer.enabled", "true")
  }

  private def checkNativeWrite(sqlStr: String, checkNative: Boolean): Unit = {
    var nativeUsed = false
    val queryListener = new QueryExecutionListener {
      override def onFailure(f: String, qe: QueryExecution, e: Exception): Unit = {}
      override def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
        if (!nativeUsed) {
          nativeUsed = if (isSparkVersionGE("3.4")) {
            qe.executedPlan.find(_.isInstanceOf[ColumnarWriteFilesExec]).isDefined
          } else {
            qe.executedPlan.find(_.isInstanceOf[FakeRowAdaptor]).isDefined
          }
        }
      }
    }
    try {
      spark.listenerManager.register(queryListener)
      spark.sql(sqlStr)
      spark.sparkContext.listenerBus.waitUntilEmpty()
      if (checkNative) {
        assert(nativeUsed)
      }
    } finally {
      spark.listenerManager.unregister(queryListener)
    }
  }

  test("test hive static partition write table") {
    withTable("t") {
      spark.sql(
        "CREATE TABLE t (c int, d long, e long)" +
          " STORED AS PARQUET partitioned by (c, d)")
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "true") {
        checkNativeWrite(
          "INSERT OVERWRITE TABLE t partition(c=1, d=2)" +
            " SELECT 3 as e",
          checkNative = true)
      }
      checkAnswer(spark.table("t"), Row(3, 1, 2))
    }
  }

  test("test hive dynamic and static partition write table") {
    withTable("t") {
      spark.sql(
        "CREATE TABLE t (c int, d long, e long)" +
          " STORED AS PARQUET partitioned by (c, d)")
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "true") {
        checkNativeWrite(
          "INSERT OVERWRITE TABLE t partition(c=1, d)" +
            " SELECT 3 as e, 2 as e",
          checkNative = false)
      }
      checkAnswer(spark.table("t"), Row(3, 1, 2))
    }
  }

  test("test hive write table") {
    withTable("t") {
      spark.sql("CREATE TABLE t (c int) STORED AS PARQUET")
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "false") {
        checkNativeWrite("INSERT OVERWRITE TABLE t SELECT 1 as c", checkNative = true)
      }
      checkAnswer(spark.table("t"), Row(1))
    }
  }

  test("test hive write dir") {
    withTempPath {
      f =>
        // compatible with Spark3.3 and later
        withSQLConf("spark.sql.hive.convertMetastoreInsertDir" -> "false") {
          if (isSparkVersionGE("3.4")) {
            checkNativeWrite(
              s"""
                 |INSERT OVERWRITE DIRECTORY '${f.getCanonicalPath}' STORED AS PARQUET SELECT 1 as c
                 |""".stripMargin,
              checkNative = false
            )
          } else {
            checkNativeWrite(
              s"""
                 |INSERT OVERWRITE DIRECTORY '${f.getCanonicalPath}' STORED AS PARQUET SELECT 1 as c
                 |""".stripMargin,
              checkNative = true
            )
          }
          checkAnswer(spark.read.parquet(f.getCanonicalPath), Row(1))
        }
    }
  }

  test("select plain hive table") {
    withTable("t") {
      sql("CREATE TABLE t AS SELECT 1 as c")
      checkAnswer(sql("SELECT * FROM t"), Row(1))
    }
  }

  test("native writer support CreateHiveTableAsSelectCommand") {
    withTable("t") {
      withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "false") {
        checkNativeWrite("CREATE TABLE t STORED AS PARQUET AS SELECT 1 as c", checkNative = true)
      }
      checkAnswer(spark.table("t"), Row(1))
    }
  }

  test("native writer should respect table properties") {
    Seq(true, false).foreach {
      enableNativeWrite =>
        withSQLConf("spark.gluten.sql.native.writer.enabled" -> enableNativeWrite.toString) {
          withTable("t") {
            withSQLConf(
              "spark.sql.hive.convertMetastoreParquet" -> "false",
              "spark.sql.parquet.compression.codec" -> "gzip") {
              checkNativeWrite(
                "CREATE TABLE t STORED AS PARQUET TBLPROPERTIES ('parquet.compression'='zstd') " +
                  "AS SELECT 1 as c",
                checkNative = enableNativeWrite)
              val tableDir = new Path(s"${conf.getConf(StaticSQLConf.WAREHOUSE_PATH)}/t")
              val configuration = spark.sessionState.newHadoopConf()
              val files = tableDir
                .getFileSystem(configuration)
                .listStatus(tableDir)
                .filterNot(_.getPath.getName.startsWith("\\."))
              assert(files.nonEmpty)
              val in = HadoopInputFile.fromStatus(files.head, spark.sessionState.newHadoopConf())
              Utils.tryWithResource(ParquetFileReader.open(in)) {
                reader =>
                  val column = reader.getFooter.getBlocks.get(0).getColumns.get(0)
                  // native writer and vanilla spark hive writer should be consistent
                  "zstd".equalsIgnoreCase(column.getCodec.toString)
              }
            }
          }
        }
    }
  }

  test("Native writer support compatible hive bucket write with dynamic partition") {
    if (isSparkVersionGE("3.4")) {
      Seq("true", "false").foreach {
        enableConvertMetastore =>
          withSQLConf("spark.sql.hive.convertMetastoreParquet" -> enableConvertMetastore) {
            val source = "hive_source_table"
            val target = "hive_bucketed_table"
            withTable(source, target) {
              sql(s"""
                     |CREATE TABLE IF NOT EXISTS $target (i int, j string)
                     |PARTITIONED BY(k string)
                     |CLUSTERED BY (i, j) SORTED BY (i) INTO 8 BUCKETS
                     |STORED AS PARQUET
               """.stripMargin)

              val df =
                (0 until 50).map(i => (i % 13, i.toString, i % 5)).toDF("i", "j", "k")
              df.write.mode(SaveMode.Overwrite).saveAsTable(source)

              withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
                checkNativeWrite(s"INSERT INTO $target SELECT * FROM $source", checkNative = true)
              }

              for (k <- 0 until 5) {
                testBucketing(
                  new File(tableDir(target), s"k=$k"),
                  "parquet",
                  8,
                  Seq("i", "j"),
                  Seq("i"),
                  df,
                  bucketIdExpression,
                  getBucketIdFromFileName)
              }
            }
          }
      }
    }
  }

  test("bucket writer with non-dynamic partition should fallback") {
    if (isSparkVersionGE("3.4")) {
      Seq("true", "false").foreach {
        enableConvertMetastore =>
          withSQLConf("spark.sql.hive.convertMetastoreParquet" -> enableConvertMetastore) {
            val source = "hive_source_table"
            val target = "hive_bucketed_table"
            withTable(source, target) {
              sql(s"""
                     |CREATE TABLE IF NOT EXISTS $target (i int, j string)
                     |PARTITIONED BY(k string)
                     |CLUSTERED BY (i, j) SORTED BY (i) INTO 8 BUCKETS
                     |STORED AS PARQUET
               """.stripMargin)

              val df =
                (0 until 50).map(i => (i % 13, i.toString, i % 5)).toDF("i", "j", "k")
              df.write.mode(SaveMode.Overwrite).saveAsTable(source)

              // hive relation convert always use dynamic, so it will offload to native.
              checkNativeWrite(
                s"INSERT INTO $target PARTITION(k='0') SELECT i, j FROM $source",
                checkNative = enableConvertMetastore.toBoolean)
              val files = tableDir(target)
                .listFiles()
                .filterNot(f => f.getName.startsWith(".") || f.getName.startsWith("_"))
              assert(files.length == 1 && files.head.getName.contains("k=0"))
              checkAnswer(spark.table(target).select("i", "j"), df.select("i", "j"))
            }
          }
      }
    }
  }

  test("bucket writer with non-partition table should fallback") {
    if (isSparkVersionGE("3.4")) {
      Seq("true", "false").foreach {
        enableConvertMetastore =>
          withSQLConf("spark.sql.hive.convertMetastoreParquet" -> enableConvertMetastore) {
            val source = "hive_source_table"
            val target = "hive_bucketed_table"
            withTable(source, target) {
              sql(s"""
                     |CREATE TABLE IF NOT EXISTS $target (i int, j string)
                     |CLUSTERED BY (i, j) SORTED BY (i) INTO 8 BUCKETS
                     |STORED AS PARQUET
               """.stripMargin)

              val df =
                (0 until 50).map(i => (i % 13, i.toString)).toDF("i", "j")
              df.write.mode(SaveMode.Overwrite).saveAsTable(source)

              checkNativeWrite(s"INSERT INTO $target SELECT i, j FROM $source", checkNative = false)

              checkAnswer(spark.table(target), df)
            }
          }
      }
    }
  }
}
