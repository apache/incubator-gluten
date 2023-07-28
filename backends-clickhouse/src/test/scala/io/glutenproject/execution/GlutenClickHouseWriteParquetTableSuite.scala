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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.clickhouse.ClickHouseLog
import org.apache.spark.sql.test.SharedSparkSession

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.sql.{Date, Timestamp}

class GlutenClickHouseWriteParquetTableSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper
  with SharedSparkSession
  with BeforeAndAfterAll {

  private var _hiveSpark: SparkSession = _

  override protected val resourcePath: String =
    "../../../../gluten-core/src/test/resources/tpch-data"

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  override protected def createTPCHNullableTables(): Unit = {}

  override protected def createTPCHNotNullTables(): Unit = {}

  override protected def sparkConf: SparkConf = {
    new SparkConf()
      .set("spark.plugins", "io.glutenproject.GlutenPlugin")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "536870912")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.files.minPartitionNum", "1")
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
      // TODO: support default ANSI policy
      .set("spark.sql.storeAssignmentPolicy", "legacy")
      // .set("spark.gluten.sql.columnar.backend.ch.runtime_config.logger.level", "debug")
      .set("spark.sql.warehouse.dir", getWarehouseDir)
      .setMaster("local[1]")
  }

  private def getWarehouseDir = {
    // test non-ascii path, by the way
    // scalastyle:off nonascii
    basePath + "/中文/spark-warehouse"
  }

  override protected def spark: SparkSession = _hiveSpark

  override protected def initializeSession(): Unit = {
    if (_hiveSpark == null) {
      val hiveMetaStoreDB = metaStorePathAbsolute + "/metastore_db"
      _hiveSpark = SparkSession
        .builder()
        .config(sparkConf)
        .enableHiveSupport()
        .config(
          "javax.jdo.option.ConnectionURL",
          s"jdbc:derby:;databaseName=$hiveMetaStoreDB;create=true")
        .getOrCreate()
    }
  }

  private val parquet_table_name = "hive_parquet_test"
  private val parquet_table_name_vanilla = "hive_parquet_test_written_by_vanilla"

  def genTestData(): Seq[AllDataTypesWithComplextType] = {
    (0 to 199).map {
      i =>
        if (i % 100 == 1) {
          AllDataTypesWithComplextType()
        } else {
          AllDataTypesWithComplextType(
            s"$i",
            i,
            i.toLong,
            i.toFloat,
            i.toDouble,
            i.toShort,
            i.toByte,
            i % 2 == 0,
            new java.math.BigDecimal(i + ".56"),
            Date.valueOf(new Date(System.currentTimeMillis()).toLocalDate.plusDays(i % 10)),
            Timestamp.valueOf(
              new Timestamp(System.currentTimeMillis()).toLocalDateTime.plusDays(i % 10)),
            Seq.apply(i + 1, i + 2, i + 3),
            Seq.apply(Option.apply(i + 1), Option.empty, Option.apply(i + 3)),
            Map.apply((i + 1, i + 2), (i + 3, i + 4)),
            Map.empty
          )
        }
    }
  }

  protected def initializeTable(table_name: String, table_create_sql: String): Unit = {
    spark.createDataFrame(genTestData()).createOrReplaceTempView("tmp_t")
    spark.sql(s"drop table IF EXISTS $table_name")
    spark.sql(table_create_sql)
    spark.sql("insert into %s select * from tmp_t".format(table_name))
  }

  override def beforeAll(): Unit = {
    // prepare working paths
    val basePathDir = new File(basePath)
    if (basePathDir.exists()) {
      FileUtils.forceDelete(basePathDir)
    }
    FileUtils.forceMkdir(basePathDir)
    FileUtils.forceMkdir(new File(warehouse))
    FileUtils.forceMkdir(new File(metaStorePathAbsolute))
    FileUtils.copyDirectory(new File(rootPath + resourcePath), new File(tablesPath))
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    ClickHouseLog.clearCache()

    try {
      super.afterAll()
    } finally {
      try {
        if (_hiveSpark != null) {
          try {
            _hiveSpark.sessionState.catalog.reset()
          } finally {
            _hiveSpark.stop()
            _hiveSpark = null
          }
        }
      } finally {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }
  }

  def getColumnName(s: String): String = {
    s.replaceAll("\\(", "_").replaceAll("\\)", "_")
  }
  import collection.immutable.ListMap

  import java.io.File

  def writeIntoNewTableWithSql(parquet_table_create_sql: String)(fields: Seq[String]): Unit = {
    spark.sql(parquet_table_create_sql)
    spark.sql(
      s"insert overwrite $parquet_table_name select ${fields.mkString(",")}" +
        s" from origin_table")
  }

  def writeAndCheckRead(write: Seq[String] => Unit, fields: Seq[String]): Unit = {
    val originDF = spark.createDataFrame(genTestData())
    originDF.createOrReplaceTempView("origin_table")
    spark.sql(s"drop table IF EXISTS $parquet_table_name")

    val rowsFromOriginTable =
      spark.sql(s"select ${fields.mkString(",")} from origin_table").collect()
    // write them to parquet table
    write(fields)

    val dfFromWriteTable =
      spark.sql(
        s"select " +
          s"${fields
              .map(getColumnName)
              .mkString(",")} " +
          s"from $parquet_table_name")
    checkAnswer(dfFromWriteTable, rowsFromOriginTable)
  }

  test("test insert into dir") {
    withSQLConf(
      ("spark.gluten.sql.native.parquet.writer.enabled", "true"),
      ("spark.gluten.enabled", "true")) {

      val originDF = spark.createDataFrame(genTestData())
      originDF.createOrReplaceTempView("origin_table")

      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date")
      )

      spark.sql(
        "insert overwrite local directory '/tmp/test_insert_into_dir1' stored as parquet select "
          + fields.keys.mkString(",") +
          " from origin_table cluster by (byte_field)")
      spark.sql(
        "insert overwrite local directory '/tmp/test_insert_into_dir2' " +
          "stored as parquet " +
          "select string_field, sum(int_field) as x from origin_table group by string_field")
    }
  }

  test("test insert into partition") {
    withSQLConf(
      ("spark.gluten.sql.native.parquet.writer.enabled", "true"),
      ("spark.gluten.enabled", "true")) {

      val originDF = spark.createDataFrame(genTestData())
      originDF.createOrReplaceTempView("origin_table")
      spark.sql(s"drop table IF EXISTS $parquet_table_name")

      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date")
      )

      val parquet_table_create_sql =
        s"create table if not exists $parquet_table_name (" +
          fields
            .map(f => s"${f._1} ${f._2}")
            .mkString(",") +
          " ) partitioned by (another_date_field date) " +
          "stored as parquet"

      spark.sql(parquet_table_create_sql)

      spark.sql(
        s"insert into $parquet_table_name partition(another_date_field = '2020-01-01') select "
          + fields.keys.mkString(",") +
          " from origin_table")

      val files = recursiveListFiles(new File(getWarehouseDir + "/" + parquet_table_name))
        .filter(_.getName.endsWith(".parquet"))
      assert(files.length == 1)
      assert(files.head.getAbsolutePath.contains("another_date_field=2020-01-01"))
    }
  }

  test("test CTAS") {
    withSQLConf(
      ("spark.gluten.sql.native.parquet.writer.enabled", "true"),
      ("spark.gluten.enabled", "true")) {

      val originDF = spark.createDataFrame(genTestData())
      originDF.createOrReplaceTempView("origin_table")
      spark.sql(s"drop table IF EXISTS $parquet_table_name")

      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date")
      )

      val parquet_table_create_sql =
        s"create table $parquet_table_name using parquet as select " +
          fields
            .map(f => s"${f._1}")
            .mkString(",") +
          " from origin_table"
      spark.sql(parquet_table_create_sql)

      spark.sql(s"drop table IF EXISTS $parquet_table_name")

      try {
        val parquet_table_create_sql =
          s"create table $parquet_table_name as select " +
            fields
              .map(f => s"${f._1}")
              .mkString(",") +
            " from origin_table"
        spark.sql(parquet_table_create_sql)
      } catch {
        case _: UnsupportedOperationException => // expected
        case _: Exception => fail("should not throw exception")
      }

    }
  }

  test("test insert into partition, bigo's case which incur InsertIntoHiveTable") {
    withSQLConf(
      ("spark.gluten.sql.native.parquet.writer.enabled", "true"),
      ("spark.sql.hive.convertMetastoreParquet", "false"),
      ("spark.gluten.enabled", "true")) {
      val originDF = spark.createDataFrame(genTestData())
      originDF.createOrReplaceTempView("origin_table")
      spark.sql(s"drop table IF EXISTS $parquet_table_name")
      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date")
      )
      val parquet_table_create_sql = s"create table if not exists $parquet_table_name (" + fields
        .map(f => s"${f._1} ${f._2}")
        .mkString(",") + " ) partitioned by (another_date_field string)" +
        "stored as parquet"

      spark.sql(parquet_table_create_sql)
//      spark.sql(s"describe table extended $parquet_table_name ").show(100, false)
      spark.sql(
        s"insert overwrite table $parquet_table_name " +
          "partition(another_date_field = '2020-01-01') select "
          + fields.keys.mkString(",") + " from (select " + fields.keys.mkString(
            ",") + ", row_number() over (order by int_field desc) as rn  " +
          "from origin_table where float_field > 3 ) tt where rn <= 100")
      val files = recursiveListFiles(new File(getWarehouseDir + "/" + parquet_table_name))
        .filter(_.getName.startsWith("part"))
      assert(files.length == 1)
      assert(files.head.getAbsolutePath.contains("another_date_field=2020-01-01"))
    }
  }

  test("test 1-col partitioned table") {
    withSQLConf(("spark.gluten.sql.native.parquet.writer.enabled", "true")) {

      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date")
      )

      val parquet_table_create_sql =
        s"create table if not exists $parquet_table_name (" +
          fields
            .filterNot(e => e._1.equals("date_field"))
            .map(f => s"${f._1} ${f._2}")
            .mkString(",") +
          " ) partitioned by (date_field date) " +
          "stored as parquet"

      writeAndCheckRead(writeIntoNewTableWithSql(parquet_table_create_sql), fields.keys.toSeq)
    }
  }

  // even if disable native writer, this UT fail, spark bug???
  ignore("test 1-col partitioned table, partitioned by already ordered column") {
    withSQLConf(("spark.gluten.sql.native.parquet.writer.enabled", "false")) {

      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date")
      )

      val parquet_table_create_sql =
        s"create table if not exists $parquet_table_name (" +
          fields
            .filterNot(e => e._1.equals("date_field"))
            .map(f => s"${f._1} ${f._2}")
            .mkString(",") +
          " ) partitioned by (date_field date) " +
          "stored as parquet"

      val originDF = spark.createDataFrame(genTestData())
      originDF.createOrReplaceTempView("origin_table")
      spark.sql(s"drop table IF EXISTS $parquet_table_name")
      spark.sql(parquet_table_create_sql)
      spark.sql(
        s"insert overwrite $parquet_table_name select ${fields.mkString(",")}" +
          s" from origin_table order by date_field")
    }
  }

  test("test 2-col partitioned table") {
    withSQLConf(
      ("spark.gluten.sql.native.parquet.writer.enabled", "true"),
      ("spark.gluten.enabled", "true")) {

      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date"),
        ("byte_field", "byte")
      )

      val parquet_table_create_sql =
        s"create table if not exists $parquet_table_name (" +
          fields
            .filterNot(e => e._1.equals("date_field") || e._1.equals("byte_field"))
            .map(f => s"${f._1} ${f._2}")
            .mkString(",") + " ) partitioned by (date_field date, byte_field byte) " +
          "stored as parquet"

      writeAndCheckRead(writeIntoNewTableWithSql(parquet_table_create_sql), fields.keys.toSeq)
    }
  }

  ignore(
    "test hive parquet table, all types of columns being partitioned except the date_field," +
      " ignore because takes too long") {
    withSQLConf(
      ("spark.gluten.sql.native.parquet.writer.enabled", "true"),
      ("spark.gluten.enabled", "true")) {

      val fields: ListMap[String, String] = ListMap(
        ("date_field", "date"),
        ("timestamp_field", "timestamp"),
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)")
      )

      for (field <- fields.filterNot(e => e._1.equals("date_field"))) {
        spark.sql(s"drop table if exists $parquet_table_name")
        val parquet_table_create_sql =
          s"create table if not exists $parquet_table_name (" +
            " date_field date" + " ) partitioned by (" +
            field._1 + " " + field._2 +
            ") " +
            "stored as parquet"

        writeAndCheckRead(
          writeIntoNewTableWithSql(parquet_table_create_sql),
          List("date_field", field._1))
      }

    }
  }

  test("test hive parquet table, all columns being partitioned. ") {
    withSQLConf(
      ("spark.gluten.sql.native.parquet.writer.enabled", "true"),
      ("spark.gluten.enabled", "true")) {

      val fields: ListMap[String, String] = ListMap(
        ("date_field", "date"),
        ("timestamp_field", "timestamp"),
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)")
      )

      val parquet_table_create_sql =
        s"create table if not exists $parquet_table_name (" +
          " date_field date" + " ) partitioned by (" +
          fields
            .filterNot(e => e._1.equals("date_field"))
            .map(f => s"${f._1} ${f._2}")
            .mkString(",") +
          ") " +
          "stored as parquet"

      writeAndCheckRead(writeIntoNewTableWithSql(parquet_table_create_sql), fields.keys.toSeq)
    }
  }

  test(("test hive parquet table with aggregated results")) {
    withSQLConf(
      ("spark.gluten.sql.native.parquet.writer.enabled", "true"),
      ("spark.gluten.enabled", "true")) {

      val fields: ListMap[String, String] = ListMap(
        ("sum(int_field)", "bigint")
      )

      val parquet_table_create_sql =
        s"create table if not exists $parquet_table_name (" +
          fields
            .map(f => s"${getColumnName(f._1)} ${f._2}")
            .mkString(",") +
          " ) stored as parquet"

      writeAndCheckRead(writeIntoNewTableWithSql(parquet_table_create_sql), fields.keys.toSeq)
    }
  }

  test("test 1-col partitioned + 1-col bucketed table") {
    withSQLConf(
      ("spark.gluten.sql.native.parquet.writer.enabled", "true"),
      ("spark.gluten.enabled", "true")) {

      val fields: ListMap[String, String] = ListMap(
        ("string_field", "string"),
        ("int_field", "int"),
        ("long_field", "long"),
        ("float_field", "float"),
        ("double_field", "double"),
        ("short_field", "short"),
        ("byte_field", "byte"),
        ("boolean_field", "boolean"),
        ("decimal_field", "decimal(23,12)"),
        ("date_field", "date")
      )

      // spark write does not support bucketed table
      // https://issues.apache.org/jira/browse/SPARK-19256
      writeAndCheckRead(
        fields => {
          spark
            .table("origin_table")
            .select(fields.head, fields.tail: _*)
            .write
            .partitionBy("date_field")
            .bucketBy(2, "byte_field")
            .saveAsTable(parquet_table_name)
        },
        fields.keys.toSeq
      )

      assert(
        new File(getWarehouseDir + "/" + parquet_table_name)
          .listFiles()
          .filter(_.isDirectory)
          .filter(!_.getName.equals("date_field=__HIVE_DEFAULT_PARTITION__"))
          .head
          .listFiles()
          .length == 2
      ) // 2 bucket files
    }
  }

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  def getSignature(filesOfNativeWriter: Array[File]): Array[(Long, Long)] = {
    filesOfNativeWriter.map(
      f => {
        val df = spark.read.parquet(f.getAbsolutePath)
        (
          df.count(),
          df.agg(("int_field", "sum")).collect().apply(0).apply(0).asInstanceOf[Long]
        )
      })
  }

  test("test table bucketed by all typed columns") {
    val fields: ListMap[String, String] = ListMap(
      ("string_field", "string"),
      ("int_field", "int"),
      ("long_field", "long"),
      ("float_field", "float"),
      ("double_field", "double"),
      ("short_field", "short"),
      ("byte_field", "byte"),
      ("boolean_field", "boolean"),
      ("decimal_field", "decimal(23,12)"),
      ("date_field", "date"),
      ("timestamp_field", "timestamp")
    )

//    for (bucketField <- fields.keys)
    {
      withSQLConf(("spark.gluten.sql.native.parquet.writer.enabled", "true")) {
        writeAndCheckRead(
          fields => {
            spark
              .table("origin_table")
              .select(fields.head, fields.tail: _*)
              .write
              .bucketBy(10, fields.head, fields.tail: _*)
//              .bucketBy(10, bucketField)
              .saveAsTable(parquet_table_name)
          },
          fields.keys.toSeq
        )
      }

      withSQLConf(("spark.gluten.sql.native.parquet.writer.enabled", "true")) {
        spark.sql(s"drop table IF EXISTS $parquet_table_name_vanilla")
        spark
          .table("origin_table")
          .select(fields.keys.toSeq.head, fields.keys.toSeq.tail: _*)
          .write
          .bucketBy(10, fields.keys.toSeq.head, fields.keys.toSeq.tail: _*)
//          .bucketBy(10, bucketField)
          .saveAsTable(parquet_table_name_vanilla)

        val sigsOfNativeWriter =
          getSignature(
            recursiveListFiles(new File(getWarehouseDir + "/" + parquet_table_name))
              .filter(_.getName.endsWith(".parquet"))).sorted
        val sigsOfVanillaWriter =
          getSignature(
            recursiveListFiles(new File(getWarehouseDir + "/" + parquet_table_name_vanilla))
              .filter(_.getName.endsWith(".parquet"))).sorted

        assert(sigsOfVanillaWriter.sameElements(sigsOfNativeWriter))
      }
    }
  }

  test("test 1-col partitioned + 2-col bucketed table") {
    val fields: ListMap[String, String] = ListMap(
      ("string_field", "string"),
      ("int_field", "int"),
      ("long_field", "long"),
      ("float_field", "float"),
      ("double_field", "double"),
      ("short_field", "short"),
      ("byte_field", "byte"),
      ("boolean_field", "boolean"),
      ("decimal_field", "decimal(23,12)"),
      ("date_field", "date")
      // TODO(liyang)
//      ("array", "array<int>"),
//      ("map", "map<int, long>")
    )

    withSQLConf(("spark.gluten.sql.native.parquet.writer.enabled", "true")) {
      writeAndCheckRead(
        fields => {
          spark
            .table("origin_table")
            .select(fields.head, fields.tail: _*)
            .write
            .partitionBy("date_field")
            .bucketBy(10, "byte_field", "string_field")
            .saveAsTable(parquet_table_name)
        },
        fields.keys.toSeq
      )
    }

    withSQLConf(("spark.gluten.sql.native.parquet.writer.enabled", "true")) {
      spark.sql(s"drop table IF EXISTS $parquet_table_name_vanilla")
      spark
        .table("origin_table")
        .select(fields.keys.toSeq.head, fields.keys.toSeq.tail: _*)
        .write
        .partitionBy("date_field")
        .bucketBy(10, "byte_field", "string_field")
        .saveAsTable(parquet_table_name_vanilla)

      val sigsOfNativeWriter =
        getSignature(
          recursiveListFiles(new File(getWarehouseDir + "/" + parquet_table_name))
            .filter(_.getName.endsWith(".parquet"))).sorted
      val sigsOfVanillaWriter =
        getSignature(
          recursiveListFiles(new File(getWarehouseDir + "/" + parquet_table_name_vanilla))
            .filter(_.getName.endsWith(".parquet"))).sorted

      assert(sigsOfVanillaWriter.sameElements(sigsOfNativeWriter))
    }
  }

  test("test consecutive blocks having same partition value") {
    withSQLConf(("spark.gluten.sql.native.parquet.writer.enabled", "true")) {

      spark.sql(s"drop table IF EXISTS $parquet_table_name")

      // 8096 row per block, so there will be blocks containing all 0 in p, all 1 in p
      spark
        .range(30000)
        .selectExpr("id", "id % 2 as p")
        .orderBy("p")
        .write
        .partitionBy("p")
        .saveAsTable(parquet_table_name)

      val ret = spark.sql("select sum(id) from " + parquet_table_name).collect().apply(0).apply(0)
      assert(ret == 449985000)
    }
  }

  test("test decimal with rand()") {
    withSQLConf(("spark.gluten.sql.native.parquet.writer.enabled", "true")) {
      spark.sql(s"drop table IF EXISTS $parquet_table_name")

      spark
        .range(200)
        .selectExpr("id", " cast((id + rand()) as decimal(23,12)) as p")
        .write
        .partitionBy("p")
        .saveAsTable(parquet_table_name)
      val ret = spark.sql("select max(p) from " + parquet_table_name).collect().apply(0).apply(0)

    }
  }

  test("test partitioned by constant") {
    withSQLConf(("spark.gluten.sql.native.parquet.writer.enabled", "true")) {

      spark.sql(s"drop table IF EXISTS tmp_lzl_screen_pic_convert_save")
      spark.sql(
        s"create table tmp(" +
          s"x1 string, x2 bigint,x3 string, x4 bigint, x5 string )" +
          s"partitioned by (day date) stored as parquet")

      spark.sql("insert into tmp partition(day) " +
        "select cast(id as string), id, cast(id as string), id, cast(id as string), '2023-05-09' " +
        "from range(10000000)")
    }
  }

  test("test bucketed by constant") {
    withSQLConf(("spark.gluten.sql.native.parquet.writer.enabled", "true")) {

      spark.sql(s"drop table IF EXISTS $parquet_table_name")

      spark
        .range(10000000)
        .selectExpr("id", "cast('2020-01-01' as date) as p")
        .write
        .bucketBy(2, "p")
        .saveAsTable(parquet_table_name)

      val ret = spark.sql("select count(*) from " + parquet_table_name).collect().apply(0).apply(0)
    }
  }

  test("test consecutive null values being partitioned") {
    withSQLConf(("spark.gluten.sql.native.parquet.writer.enabled", "true")) {

      spark.sql(s"drop table IF EXISTS $parquet_table_name")

      spark
        .range(30000)
        .selectExpr("id", "cast(null as string) as p")
        .write
        .partitionBy("p")
        .saveAsTable(parquet_table_name)

      val ret = spark.sql("select count(*) from " + parquet_table_name).collect().apply(0).apply(0)
    }
  }

  test("test consecutive null values being bucketed") {
    withSQLConf(("spark.gluten.sql.native.parquet.writer.enabled", "true")) {

      spark.sql(s"drop table IF EXISTS $parquet_table_name")

      spark
        .range(30000)
        .selectExpr("id", "cast(null as string) as p")
        .write
        .bucketBy(2, "p")
        .saveAsTable(parquet_table_name)

      val ret = spark.sql("select count(*) from " + parquet_table_name).collect().apply(0).apply(0)
    }
  }

}
