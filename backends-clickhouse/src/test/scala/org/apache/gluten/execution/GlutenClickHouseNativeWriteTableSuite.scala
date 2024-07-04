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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.execution.AllDataTypesWithComplexType.genTestData
import org.apache.gluten.utils.UTSystemParameters

import org.apache.spark.SparkConf
import org.apache.spark.gluten.NativeWriteChecker
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.SharedSparkSession

import org.scalatest.BeforeAndAfterAll

import scala.reflect.runtime.universe.TypeTag

class GlutenClickHouseNativeWriteTableSuite
  extends GlutenClickHouseWholeStageTransformerSuite
  with AdaptiveSparkPlanHelper
  with SharedSparkSession
  with BeforeAndAfterAll
  with NativeWriteChecker {

  private var _hiveSpark: SparkSession = _

  override protected def sparkConf: SparkConf = {
    new SparkConf()
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1073741824")
      .set("spark.sql.catalogImplementation", "hive")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.minPartitionNum", "1")
      .set("spark.databricks.delta.maxSnapshotLineageLength", "20")
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
      .set("spark.databricks.delta.stalenessLimit", "3600000")
      .set("spark.gluten.sql.columnar.columnartorow", "true")
      .set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
      .set(GlutenConfig.GLUTEN_LIB_PATH, UTSystemParameters.clickHouseLibPath)
      .set("spark.gluten.sql.columnar.iterator", "true")
      .set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
      .set("spark.gluten.sql.enable.native.validation", "false")
      // TODO: support default ANSI policy
      .set("spark.sql.storeAssignmentPolicy", "legacy")
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

  private val table_name_template = "hive_%s_test"
  private val table_name_vanilla_template = "hive_%s_test_written_by_vanilla"
  private val formats = Array("orc", "parquet")

  override protected def afterAll(): Unit = {
    DeltaLog.clearCache()

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

  def writeIntoNewTableWithSql(table_name: String, table_create_sql: String)(
      fields: Seq[String]): Unit = {
    withDestinationTable(table_name, table_create_sql) {
      checkNativeWrite(
        s"insert overwrite $table_name select ${fields.mkString(",")}" +
          s" from origin_table",
        checkNative = true)
    }
  }

  def writeAndCheckRead(
      table_name: String,
      write: Seq[String] => Unit,
      fields: Seq[String]): Unit = {
    val originDF = spark.createDataFrame(genTestData())
    originDF.createOrReplaceTempView("origin_table")

    spark.sql(s"drop table IF EXISTS $table_name")

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
          s"from $table_name")
    checkAnswer(dfFromWriteTable, rowsFromOriginTable)
  }

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }

  def getSignature(format: String, filesOfNativeWriter: Array[File]): Array[(Long, Long)] = {
    filesOfNativeWriter.map(
      f => {
        val df = if (format.equals("parquet")) {
          spark.read.parquet(f.getAbsolutePath)
        } else {
          spark.read.orc(f.getAbsolutePath)
        }
        (
          df.count(),
          df.agg(("int_field", "sum")).collect().apply(0).apply(0).asInstanceOf[Long]
        )
      })
  }

  private val fields_ = ListMap(
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

  def withDestinationTable(table: String, createTableSql: String)(f: => Unit): Unit = {
    spark.sql(s"drop table IF EXISTS $table")
    spark.sql(s"$createTableSql")
    f
  }

  def nativeWrite(f: String => Unit): Unit = {
    withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
      formats.foreach(f(_))
    }
  }

  def nativeWrite2(
      f: String => (String, String, String),
      extraCheck: (String, String, String) => Unit = null): Unit = nativeWrite {
    format =>
      val (table_name, table_create_sql, insert_sql) = f(format)
      withDestinationTable(table_name, table_create_sql) {
        checkNativeWrite(insert_sql, checkNative = true)
        Option(extraCheck).foreach(_(table_name, table_create_sql, insert_sql))
      }
  }

  def nativeWriteWithOriginalView[A <: Product: TypeTag](
      data: Seq[A],
      viewName: String,
      pairs: (String, String)*)(f: String => Unit): Unit = {
    val configs = pairs :+ ("spark.gluten.sql.native.writer.enabled", "true")
    withSQLConf(configs: _*) {
      withTempView(viewName) {
        spark.createDataFrame(data).createOrReplaceTempView(viewName)
        formats.foreach(f(_))
      }
    }
  }

  test("test insert into dir") {
    nativeWriteWithOriginalView(genTestData(), "origin_table") {
      format =>
        Seq(
          s"""insert overwrite local directory '$basePath/test_insert_into_${format}_dir1'
             |stored as $format select ${fields_.keys.mkString(",")}
             |from origin_table""".stripMargin,
          s"""insert overwrite local directory '$basePath/test_insert_into_${format}_dir2'
             |stored as $format select string_field, sum(int_field) as x
             |from origin_table group by string_field""".stripMargin
        ).foreach(checkNativeWrite(_, checkNative = true))
    }
  }

  test("test insert into partition") {
    def destination(format: String): (String, String, String) = {
      val table_name = table_name_template.format(format)
      val table_create_sql =
        s"""create table if not exists $table_name
           |(${fields_.map(f => s"${f._1} ${f._2}").mkString(",")})
           |partitioned by (another_date_field date) stored as $format""".stripMargin
      val insert_sql =
        s"""insert into $table_name partition(another_date_field = '2020-01-01')
           | select ${fields_.keys.mkString(",")} from origin_table""".stripMargin
      (table_name, table_create_sql, insert_sql)
    }

    def nativeFormatWrite(format: String): Unit = {
      val (table_name, table_create_sql, insert_sql) = destination(format)
      withDestinationTable(table_name, table_create_sql) {
        checkNativeWrite(insert_sql, checkNative = true)
        var files = recursiveListFiles(new File(getWarehouseDir + "/" + table_name))
          .filter(_.getName.endsWith(s".$format"))
        if (format == "orc") {
          files = files.filter(_.getName.contains(".lz4"))
        }
        assert(files.length == 1)
        assert(files.head.getAbsolutePath.contains("another_date_field=2020-01-01"))
      }
    }

    nativeWriteWithOriginalView(
      genTestData(),
      "origin_table",
      ("spark.sql.orc.compression.codec", "lz4"))(nativeFormatWrite)
  }

  test("test CTAS") {
    nativeWriteWithOriginalView(genTestData(), "origin_table") {
      format =>
        val table_name = table_name_template.format(format)
        val table_create_sql =
          s"create table $table_name using $format as select " +
            fields_
              .map(f => s"${f._1}")
              .mkString(",") +
            " from origin_table"
        val insert_sql =
          s"create table $table_name as select " +
            fields_
              .map(f => s"${f._1}")
              .mkString(",") +
            " from origin_table"
        withDestinationTable(table_name, table_create_sql) {
          spark.sql(s"drop table IF EXISTS $table_name")

          try {
            // FIXME: using checkNativeWrite
            spark.sql(insert_sql)
          } catch {
            case _: UnsupportedOperationException => // expected
            case e: Exception => fail("should not throw exception", e)
          }
        }

    }
  }

  test("test insert into partition, bigo's case which incur InsertIntoHiveTable") {
    def destination(format: String): (String, String, String) = {
      val table_name = table_name_template.format(format)
      val table_create_sql = s"create table if not exists $table_name (" + fields_
        .map(f => s"${f._1} ${f._2}")
        .mkString(",") + " ) partitioned by (another_date_field string)" +
        s"stored as $format"
      val insert_sql =
        s"insert overwrite table $table_name " +
          "partition(another_date_field = '2020-01-01') select " +
          fields_.keys.mkString(",") + " from (select " + fields_.keys.mkString(
            ",") + ", row_number() over (order by int_field desc) as rn  " +
          "from origin_table where float_field > 3 ) tt where rn <= 100"
      (table_name, table_create_sql, insert_sql)
    }

    def nativeFormatWrite(format: String): Unit = {
      val (table_name, table_create_sql, insert_sql) = destination(format)
      withDestinationTable(table_name, table_create_sql) {
        checkNativeWrite(insert_sql, checkNative = true)
        val files = recursiveListFiles(new File(getWarehouseDir + "/" + table_name))
          .filter(_.getName.startsWith("part"))
        assert(files.length == 1)
        assert(files.head.getAbsolutePath.contains("another_date_field=2020-01-01"))
      }
    }

    nativeWriteWithOriginalView(
      genTestData(),
      "origin_table",
      ("spark.sql.hive.convertMetastoreParquet", "false"),
      ("spark.sql.hive.convertMetastoreOrc", "false"))(nativeFormatWrite)
  }

  test("test 1-col partitioned table") {
    nativeWrite {
      format =>
        {
          val table_name = table_name_template.format(format)
          val table_create_sql =
            s"create table if not exists $table_name (" +
              fields_
                .filterNot(e => e._1.equals("date_field"))
                .map(f => s"${f._1} ${f._2}")
                .mkString(",") +
              " ) partitioned by (date_field date) " +
              s"stored as $format"

          writeAndCheckRead(
            table_name,
            writeIntoNewTableWithSql(table_name, table_create_sql),
            fields_.keys.toSeq)
        }
    }
  }

  // even if disable native writer, this UT fail, spark bug???
  ignore("test 1-col partitioned table, partitioned by already ordered column") {
    withSQLConf(("spark.gluten.sql.native.writer.enabled", "false")) {
      val originDF = spark.createDataFrame(genTestData())
      originDF.createOrReplaceTempView("origin_table")

      for (format <- formats) {
        val table_name = table_name_template.format(format)
        val table_create_sql =
          s"create table if not exists $table_name (" +
            fields_
              .filterNot(e => e._1.equals("date_field"))
              .map(f => s"${f._1} ${f._2}")
              .mkString(",") +
            " ) partitioned by (date_field date) " +
            s"stored as $format"

        spark.sql(s"drop table IF EXISTS $table_name")
        spark.sql(table_create_sql)
        spark.sql(
          s"insert overwrite $table_name select ${fields_.mkString(",")}" +
            s" from origin_table order by date_field")
      }
    }
  }

  test("test 2-col partitioned table") {
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
    nativeWrite {
      format =>
        val table_name = table_name_template.format(format)
        val table_create_sql =
          s"create table if not exists $table_name (" +
            fields
              .filterNot(e => e._1.equals("date_field") || e._1.equals("byte_field"))
              .map(f => s"${f._1} ${f._2}")
              .mkString(",") + " ) partitioned by (date_field date, byte_field byte) " +
            s"stored as $format"

        writeAndCheckRead(
          table_name,
          writeIntoNewTableWithSql(table_name, table_create_sql),
          fields.keys.toSeq)
    }
  }

  ignore(
    "test hive parquet/orc table, all types of columns being partitioned except the date_field," +
      " ignore because takes too long") {
    withSQLConf(
      ("spark.gluten.sql.native.writer.enabled", "true"),
      (GlutenConfig.GLUTEN_ENABLED.key, "true")) {

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

      for (format <- formats) {
        val table_name = table_name_template.format(format)
        for (field <- fields.filterNot(e => e._1.equals("date_field"))) {
          spark.sql(s"drop table if exists $table_name")
          val table_create_sql =
            s"create table if not exists $table_name (" +
              " date_field date" + " ) partitioned by (" +
              field._1 + " " + field._2 +
              ") " +
              s"stored as $format"

          writeAndCheckRead(
            table_name,
            writeIntoNewTableWithSql(table_name, table_create_sql),
            List("date_field", field._1))
        }
      }

    }
  }

  // This test case will be failed with incorrect result randomly, ignore first.
  ignore("test hive parquet/orc table, all columns being partitioned. ") {
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
    nativeWrite {
      format =>
        val table_name = table_name_template.format(format)
        val table_create_sql =
          s"create table if not exists $table_name (" +
            " date_field date" + " ) partitioned by (" +
            fields
              .filterNot(e => e._1.equals("date_field"))
              .map(f => s"${f._1} ${f._2}")
              .mkString(",") +
            ") " +
            s"stored as $format"

        writeAndCheckRead(
          table_name,
          writeIntoNewTableWithSql(table_name, table_create_sql),
          fields.keys.toSeq)
    }
  }

  test("test hive parquet/orc table with aggregated results") {
    val fields: ListMap[String, String] = ListMap(
      ("sum(int_field)", "bigint")
    )
    nativeWrite {
      format =>
        val table_name = table_name_template.format(format)
        val table_create_sql =
          s"create table if not exists $table_name (" +
            fields
              .map(f => s"${getColumnName(f._1)} ${f._2}")
              .mkString(",") +
            s" ) stored as $format"

        writeAndCheckRead(
          table_name,
          writeIntoNewTableWithSql(table_name, table_create_sql),
          fields.keys.toSeq)
    }
  }

  test("test 1-col partitioned + 1-col bucketed table") {
    nativeWrite {
      format =>
        // spark write does not support bucketed table
        // https://issues.apache.org/jira/browse/SPARK-19256
        val table_name = table_name_template.format(format)
        writeAndCheckRead(
          table_name,
          fields => {
            spark
              .table("origin_table")
              .select(fields.head, fields.tail: _*)
              .write
              .format(format)
              .partitionBy("date_field")
              .bucketBy(2, "byte_field")
              .saveAsTable(table_name)
          },
          fields_.keys.toSeq
        )

        assert(
          new File(getWarehouseDir + "/" + table_name)
            .listFiles()
            .filter(_.isDirectory)
            .filter(!_.getName.equals("date_field=__HIVE_DEFAULT_PARTITION__"))
            .head
            .listFiles()
            .count(!_.isHidden) == 2
        ) // 2 bucket files
    }
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

    for (format <- formats) {
      val table_name = table_name_template.format(format)
      withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
        writeAndCheckRead(
          table_name,
          fields => {
            spark
              .table("origin_table")
              .select(fields.head, fields.tail: _*)
              .write
              .format(format)
              .bucketBy(10, fields.head, fields.tail: _*)
              .saveAsTable(table_name)
          },
          fields.keys.toSeq
        )
      }

      val table_name_vanilla = table_name_vanilla_template.format(format)
      withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
        spark.sql(s"drop table IF EXISTS $table_name_vanilla")
        spark
          .table("origin_table")
          .select(fields.keys.toSeq.head, fields.keys.toSeq.tail: _*)
          .write
          .format(format)
          .bucketBy(10, fields.keys.toSeq.head, fields.keys.toSeq.tail: _*)
          .saveAsTable(table_name_vanilla)

        val sigsOfNativeWriter =
          getSignature(
            format,
            recursiveListFiles(new File(getWarehouseDir + "/" + table_name))
              .filter(_.getName.endsWith(s".$format"))).sorted
        val sigsOfVanillaWriter =
          getSignature(
            format,
            recursiveListFiles(new File(getWarehouseDir + "/" + table_name_vanilla))
              .filter(_.getName.endsWith(s".$format"))).sorted

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
      ("date_field", "date"),
      ("array", "array<int>"),
      ("map", "map<int, long>")
    )

    for (format <- formats) {
      val table_name = table_name_template.format(format)
      withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
        writeAndCheckRead(
          table_name,
          fields => {
            spark
              .table("origin_table")
              .select(fields.head, fields.tail: _*)
              .write
              .format(format)
              .partitionBy("date_field")
              .bucketBy(10, "byte_field", "string_field")
              .saveAsTable(table_name)
          },
          fields.keys.toSeq
        )
      }

      val table_name_vanilla = table_name_vanilla_template.format(format)
      withSQLConf(("spark.gluten.sql.native.writer.enabled", "true")) {
        spark.sql(s"drop table IF EXISTS $table_name_vanilla")
        spark
          .table("origin_table")
          .select(fields.keys.toSeq.head, fields.keys.toSeq.tail: _*)
          .write
          .format(format)
          .partitionBy("date_field")
          .bucketBy(10, "byte_field", "string_field")
          .saveAsTable(table_name_vanilla)

        val sigsOfNativeWriter =
          getSignature(
            format,
            recursiveListFiles(new File(getWarehouseDir + "/" + table_name))
              .filter(_.getName.endsWith(s".$format"))).sorted
        val sigsOfVanillaWriter =
          getSignature(
            format,
            recursiveListFiles(new File(getWarehouseDir + "/" + table_name_vanilla))
              .filter(_.getName.endsWith(s".$format"))).sorted

        assert(sigsOfVanillaWriter.sameElements(sigsOfNativeWriter))
      }
    }
  }

  test("test consecutive blocks having same partition value") {
    nativeWrite {
      format =>
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")

        // 8096 row per block, so there will be blocks containing all 0 in p, all 1 in p
        spark
          .range(30000)
          .selectExpr("id", "id % 2 as p")
          .orderBy("p")
          .write
          .format(format)
          .partitionBy("p")
          .saveAsTable(table_name)

        val ret = spark.sql(s"select sum(id) from $table_name").collect().apply(0).apply(0)
        assert(ret == 449985000)
    }
  }

  test("test decimal with rand()") {
    nativeWrite {
      format =>
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")
        spark
          .range(200)
          .selectExpr("id", " cast((id + rand()) as decimal(23,12)) as p")
          .write
          .format(format)
          .partitionBy("p")
          .saveAsTable(table_name)
        val ret = spark.sql(s"select max(p) from $table_name").collect().apply(0).apply(0)
    }
  }

  test("test partitioned by constant") {
    nativeWrite2 {
      format =>
        val table_name = s"tmp_123_$format"
        val create_sql =
          s"""create table tmp_123_$format(
             |x1 string, x2 bigint,x3 string, x4 bigint, x5 string )
             |partitioned by (day date) stored as $format""".stripMargin
        val insert_sql =
          s"""insert into tmp_123_$format partition(day)
             |select cast(id as string), id, cast(id as string),
             |       id, cast(id as string), '2023-05-09'
             |from range(10000000)""".stripMargin
        (table_name, create_sql, insert_sql)
    }
  }

  test("test bucketed by constant") {
    nativeWrite {
      format =>
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")

        spark
          .range(10000000)
          .selectExpr("id", "cast('2020-01-01' as date) as p")
          .write
          .format(format)
          .bucketBy(2, "p")
          .saveAsTable(table_name)

        assertResult(10000000)(spark.table(table_name).count())
    }
  }

  test("test consecutive null values being partitioned") {
    nativeWrite {
      format =>
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")

        spark
          .range(30000)
          .selectExpr("id", "cast(null as string) as p")
          .write
          .format(format)
          .partitionBy("p")
          .saveAsTable(table_name)

        assertResult(30000)(spark.table(table_name).count())
    }
  }

  test("test consecutive null values being bucketed") {
    nativeWrite {
      format =>
        val table_name = table_name_template.format(format)
        spark.sql(s"drop table IF EXISTS $table_name")

        spark
          .range(30000)
          .selectExpr("id", "cast(null as string) as p")
          .write
          .format(format)
          .bucketBy(2, "p")
          .saveAsTable(table_name)

        assertResult(30000)(spark.table(table_name).count())
    }
  }

  test("test native write with empty dataset") {
    nativeWrite2(
      format => {
        val table_name = "t_" + format
        (
          table_name,
          s"create table $table_name (id int, str string) stored as $format",
          s"insert into $table_name select id, cast(id as string) from range(10) where id > 100"
        )
      },
      (table_name, _, _) => {
        assertResult(0)(spark.table(table_name).count())
      }
    )
  }

  test("test native write with union") {
    nativeWrite {
      format =>
        val table_name = "t_" + format
        withDestinationTable(
          table_name,
          s"create table $table_name (id int, str string) stored as $format") {
          checkNativeWrite(
            s"insert overwrite table $table_name " +
              "select id, cast(id as string) from range(10) union all " +
              "select 10, '10' from range(10)",
            checkNative = true)
          checkNativeWrite(
            s"insert overwrite table $table_name " +
              "select id, cast(id as string) from range(10) union all " +
              "select 10, cast(id as string) from range(10)",
            checkNative = true
          )
        }
    }
  }

  test("test native write and non-native read consistency") {
    nativeWrite2(
      {
        format =>
          val table_name = "t_" + format
          (
            table_name,
            s"create table $table_name (id int, name string, info char(4)) stored as $format",
            s"insert overwrite table $table_name " +
              "select id, cast(id as string), concat('aaa', cast(id as string)) from range(10)"
          )
      },
      (table_name, _, _) =>
        compareResultsAgainstVanillaSpark(
          s"select * from $table_name",
          compareResult = true,
          _ => {})
    )
  }

  test("GLUTEN-4316: fix crash on dynamic partition inserting") {
    nativeWrite2 {
      format =>
        val tbl = "t_" + format
        val sql1 =
          s"create table $tbl(a int, b map<string, string>, c struct<d:string, e:string>) " +
            s"partitioned by (day string) stored as $format"
        val sql2 = s"insert overwrite $tbl partition (day) " +
          s"select id as a, str_to_map(concat('t1:','a','&t2:','b'),'&',':'), " +
          s"struct('1', null) as c, '2024-01-08' as day from range(10)"
        (tbl, sql1, sql2)
    }
  }
}
