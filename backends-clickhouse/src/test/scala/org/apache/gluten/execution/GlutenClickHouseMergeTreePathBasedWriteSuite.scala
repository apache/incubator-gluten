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
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.catalog.ClickHouseTableV2
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.v2.clickhouse.metadata.AddMergeTreeParts
import org.apache.spark.sql.functions._

import io.delta.tables.ClickhouseTable
import org.apache.commons.io.filefilter.WildcardFileFilter

import java.io.File

import scala.io.Source

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseMergeTreePathBasedWriteSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val needCopyParquetToTablePath = true

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "mergetree-queries-output"

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "20000000")
      .set("spark.ui.enabled", "true")
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_settings.min_insert_block_size_rows",
        "100000")
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createNotNullTPCHTablesInParquet(tablesPath)
  }

  test("test mergetree path based write") {
    val dataPath = s"$basePath/lineitem_filebased"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE clickhouse.`$dataPath` (
                 | l_orderkey      bigint,
                 | l_partkey       bigint,
                 | l_suppkey       bigint,
                 | l_linenumber    bigint,
                 | l_quantity      double,
                 | l_extendedprice double,
                 | l_discount      double,
                 | l_tax           double,
                 | l_returnflag    string,
                 | l_linestatus    string,
                 | l_shipdate      date,
                 | l_commitdate    date,
                 | l_receiptdate   date,
                 | l_shipinstruct  string,
                 | l_shipmode      string,
                 | l_comment       string
                 |)
                 |USING clickhouse
                 |TBLPROPERTIES (orderByKey='l_returnflag,l_shipdate',
                 |               primaryKey='l_returnflag',
                 |               lowCardKey='l_returnflag,l_linestatus')
                 |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath)
    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath)
    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Overwrite)
      .save(dataPath)

    val sqlStr =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price,
         |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
         |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         |    avg(l_quantity) AS avg_qty,
         |    avg(l_extendedprice) AS avg_price,
         |    avg(l_discount) AS avg_disc,
         |    count(*) AS count_order
         |FROM
         |    clickhouse.`$dataPath`
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runTPCHQueryBySQL(1, sqlStr) {
      df =>
        val plans = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
          case w: WholeStageTransformer => w
        }
        assert(plans.size == 4)

        val mergetreeScan = plans(3).asInstanceOf[FileSourceScanExecTransformer]
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).orderByKeyOption.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).primaryKeyOption.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assert(addFiles.size == 6)
        assert(
          addFiles.map(_.rows).sum
            == 600572)

        // GLUTEN-5060: check the unnecessary FilterExec
        val wholeStageTransformer = plans(2).asInstanceOf[WholeStageTransformer]
        val planNodeJson = wholeStageTransformer.substraitPlanJson
        assert(
          !planNodeJson
            .replaceAll("\\\n", "")
            .replaceAll(" ", "")
            .contains("\"input\":{\"filter\":{"))
    }

    val result = spark.read
      .format("clickhouse")
      .load(dataPath)
      .where("l_shipdate = date'1998-09-02'")
      .collect()
    assert(result.apply(0).get(0) == 110501)
  }

  test("test mergetree path based write with dataframe api") {
    val dataPath = s"$basePath/lineitem_filebased_df"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .option("clickhouse.orderByKey", "l_shipdate,l_orderkey")
      .option("clickhouse.primaryKey", "l_shipdate")
      .option("clickhouse.lowCardKey", "l_returnflag,l_linestatus")
      .save(dataPath)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath)
    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Overwrite)
      .option("clickhouse.orderByKey", "l_shipdate,l_orderkey")
      .option("clickhouse.primaryKey", "l_shipdate")
      .option("clickhouse.lowCardKey", "l_returnflag,l_linestatus")
      .save(dataPath)

    val sqlStr =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price,
         |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
         |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         |    avg(l_quantity) AS avg_qty,
         |    avg(l_extendedprice) AS avg_price,
         |    avg(l_discount) AS avg_disc,
         |    count(*) AS count_order
         |FROM
         |    clickhouse.`$dataPath`
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runTPCHQueryBySQL(1, sqlStr) {
      df =>
        val plans = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
          case w: WholeStageTransformer => w
        }
        assert(plans.size == 4)

        val mergetreeScan = plans(3).asInstanceOf[FileSourceScanExecTransformer]
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKeyOption
            .get
            .mkString(",")
            .equals("l_shipdate,l_orderkey"))
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .primaryKeyOption
            .get
            .mkString(",")
            .equals("l_shipdate"))
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .lowCardKeyOption
            .get
            .mkString(",")
            .equals("l_returnflag,l_linestatus"))
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assert(addFiles.size == 6)
        assert(
          addFiles.map(_.rows).sum
            == 600572)

        // GLUTEN-5060: check the unnecessary FilterExec
        val wholeStageTransformer = plans(2).asInstanceOf[WholeStageTransformer]
        val planNodeJson = wholeStageTransformer.substraitPlanJson
        assert(
          !planNodeJson
            .replaceAll("\\\n", "")
            .replaceAll(" ", "")
            .contains("\"input\":{\"filter\":{"))
    }

    val result = spark.read
      .format("clickhouse")
      .load(dataPath)
      .where("l_shipdate = date'1998-09-02'")
      .collect()
    assert(result.apply(0).get(0) == 110501)
  }

  test("test mergetree path based insert overwrite partitioned table with small table, static") {
    val dataPath = s"$basePath/lineitem_mergetree_insertoverwrite2"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .partitionBy("l_shipdate")
      .option("clickhouse.orderByKey", "l_orderkey")
      .option("clickhouse.primaryKey", "l_orderkey")
      .option("clickhouse.lowCardKey", "l_returnflag,l_linestatus")
      .save(dataPath)

    val sourceDF1 = spark.sql(
      s"""
         |select * from lineitem where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
         |""".stripMargin)
    sourceDF1.write
      .format("clickhouse")
      .mode(SaveMode.Overwrite)
      .partitionBy("l_shipdate")
      .save(dataPath)

    val result = spark.read
      .format("clickhouse")
      .load(dataPath)
      .count()
    assert(result == 2418)
  }

  test("test mergetree path based insert overwrite partitioned table with small table, dynamic") {
    withSQLConf(("spark.sql.sources.partitionOverwriteMode", "dynamic")) {
      val dataPath = s"$basePath/lineitem_mergetree_insertoverwrite3"
      clearDataPath(dataPath)

      val sourceDF = spark.sql(s"""
                                  |select * from lineitem
                                  |""".stripMargin)

      sourceDF.write
        .format("clickhouse")
        .mode(SaveMode.Append)
        .partitionBy("l_shipdate")
        .option("clickhouse.orderByKey", "l_orderkey")
        .option("clickhouse.primaryKey", "l_orderkey")
        .option("clickhouse.lowCardKey", "l_returnflag,l_linestatus")
        .save(dataPath)

      val sourceDF1 = spark.sql(
        s"""
           |select * from lineitem where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
           |""".stripMargin)
      sourceDF1.write
        .format("clickhouse")
        .mode(SaveMode.Overwrite)
        .partitionBy("l_shipdate")
        .save(dataPath)

      val result = spark.read
        .format("clickhouse")
        .load(dataPath)
        .count()
      assert(result == 600572)
    }
  }

  test("test mergetree path based table update") {
    val dataPath = s"$basePath/lineitem_mergetree_update"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath)

    spark.sql(s"""
                 | update clickhouse.`$dataPath` set l_returnflag = 'Z' where l_orderkey = 12647
                 |""".stripMargin)

    {
      val df = spark.read
        .format("clickhouse")
        .load(dataPath)
        .where("l_returnflag = 'Z'")
      assert(df.count() == 1)
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      assert(scanExec.size == 1)

      val mergetreeScan = scanExec.head
      assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

      val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
      assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
      assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
      assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).orderByKeyOption.isEmpty)
      assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).primaryKeyOption.isEmpty)
      assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
      val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
      assert(
        addFiles.map(_.rows).sum
          == 600572)

      // 4 parts belong to the first batch
      // 2 parts belong to the second batch (1 actual updated part, 1 passively updated).
      assert(addFiles.size == 6)
      val filePaths = addFiles.map(_.path).groupBy(name => name.substring(0, name.lastIndexOf("_")))
      assert(filePaths.size == 2)
      assert(Array(2, 4).sameElements(filePaths.values.map(paths => paths.size).toArray.sorted))
    }

    val clickhouseTable = ClickhouseTable.forPath(spark, dataPath)
    clickhouseTable.updateExpr("l_orderkey = 10086", Map("l_returnflag" -> "'X'"))

    {
      val df = spark.read
        .format("clickhouse")
        .load(dataPath)
        .where("l_returnflag = 'X'")
      assert(df.count() == 1)
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      assert(scanExec.size == 1)

      val mergetreeScan = scanExec.head
      assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

      val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
      val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
      assert(
        addFiles.map(_.rows).sum
          == 600572)

      // 4 parts belong to the first batch
      // 2 parts belong to the second batch (1 actual updated part, 1 passively updated).
      assert(addFiles.size == 6)
      val filePaths = addFiles.map(_.path).groupBy(name => name.substring(0, name.lastIndexOf("_")))
      assert(filePaths.size == 2)
      assert(Array(2, 4).sameElements(filePaths.values.map(paths => paths.size).toArray.sorted))
    }

    val df = spark.read
      .format("clickhouse")
      .load(dataPath)
    assert(df.count() == 600572)
  }

  test("test mergetree path based table delete") {
    val dataPath = s"$basePath/lineitem_mergetree_delete"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath)

    spark.sql(s"""
                 | delete from clickhouse.`$dataPath` where l_orderkey = 12647
                 |""".stripMargin)
    val df = spark.read
      .format("clickhouse")
      .load(dataPath)
    assert(df.count() == 600571)
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    val mergetreeScan = scanExec.head
    val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
    val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
    // 4 parts belong to the first batch
    // 2 parts belong to the second batch (1 actual updated part, 1 passively updated).
    assert(addFiles.size == 6)
    val filePaths = addFiles.map(_.path).groupBy(name => name.substring(0, name.lastIndexOf("_")))
    assert(filePaths.size == 2)
    assert(Array(2, 4).sameElements(filePaths.values.map(paths => paths.size).toArray.sorted))

    val clickhouseTable = ClickhouseTable.forPath(spark, dataPath)
    clickhouseTable.delete("mod(l_orderkey, 3) = 2")
    val df1 = spark.read
      .format("clickhouse")
      .load(dataPath)
    assert(df1.count() == 400089)
  }

  test("test mergetree path based table upsert") {
    val dataPath = s"$basePath/lineitem_mergetree_upsert"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath)

    val df0 = spark.sql(s"""
                           | select count(*) from clickhouse.`$dataPath`
                           |""".stripMargin)
    assert(
      df0.collect().apply(0).get(0) == 600572
    )
    upsertSourceTableAndCheck(dataPath)
  }

  private def upsertSourceTableAndCheck(dataPath: String) = {
    // Why selecting l_orderkey having count(*) =1 ?
    // Answer: to avoid "org.apache.spark.sql.delta.DeltaUnsupportedOperationException:
    // Cannot perform Merge as multiple source rows matched and attempted to modify the same
    // target row in the Delta table in possibly conflicting ways."
    spark.sql(s"""
          merge into clickhouse.`$dataPath`
          using (

            select l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax,
           'Z' as `l_returnflag`,
            l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment
            from lineitem where l_orderkey in (select l_orderkey from lineitem group by l_orderkey having count(*) =1 ) and l_orderkey < 100000

            union

            select l_orderkey + 10000000,
            l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag,
            l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment
            from lineitem where l_orderkey in (select l_orderkey from lineitem group by l_orderkey having count(*) =1 ) and l_orderkey < 100000

          ) as updates
          on updates.l_orderkey = clickhouse.`$dataPath`.l_orderkey
          when matched then update set *
          when not matched then insert *
          """.stripMargin)

    {
      val df1 = spark.sql(s"""
                             | select count(*) from clickhouse.`$dataPath`
                             |""".stripMargin)
      assert(
        df1.collect().apply(0).get(0) == 600572 + 3506
      )
    }
    {
      val df2 =
        spark.sql(s"""
                     | select count(*) from clickhouse.`$dataPath` where l_returnflag = 'Z'
                     |""".stripMargin)
      assert(
        df2.collect().apply(0).get(0) == 3506
      )
    }

    {
      val df3 =
        spark.sql(s"""
                     | select count(*) from clickhouse.`$dataPath` where l_orderkey > 10000000
                     |""".stripMargin)
      assert(
        df3.collect().apply(0).get(0) == 3506
      )
    }
  }

  test("test mergetree path based write with orderby keys / primary keys") {
    val dataPath = s"$basePath/lineitem_mergetree_orderbykey"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .option("clickhouse.orderByKey", "l_shipdate,l_orderkey")
      .option("clickhouse.primaryKey", "l_shipdate")
      .mode(SaveMode.Append)
      .save(dataPath)

    val sqlStr =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price,
         |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
         |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         |    avg(l_quantity) AS avg_qty,
         |    avg(l_extendedprice) AS avg_price,
         |    avg(l_discount) AS avg_disc,
         |    count(*) AS count_order
         |FROM
         |    clickhouse.`$dataPath`
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runTPCHQueryBySQL(1, sqlStr) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size == 1)

        val mergetreeScan = scanExec(0)
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKeyOption
            .get
            .mkString(",")
            .equals("l_shipdate,l_orderkey"))
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .primaryKeyOption
            .get
            .mkString(",")
            .equals("l_shipdate"))
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assert(addFiles.size == 6)
        assert(addFiles.map(_.rows).sum == 600572)
    }

    val df = spark.read
      .format("clickhouse")
      .load(dataPath)
      .where("""
               |l_shipdate >= date'1994-01-01'
               |    AND l_shipdate < date'1994-01-01' + interval 1 year
               |    AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
               |    AND l_quantity < 24
               |""".stripMargin)
      .agg(sum("l_linenumber").alias("res"))
    val result = df.collect()
    assert(result(0).getLong(0) == 34842)
  }

  test("test mergetree path based write with partition") {
    val dataPath = s"$basePath/lineitem_mergetree_partition"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .partitionBy("l_shipdate", "l_returnflag")
      .option("clickhouse.orderByKey", "l_orderkey")
      .option("clickhouse.primaryKey", "l_orderkey")
      .mode(SaveMode.Append)
      .save(dataPath)

    val sourceDF1 = spark.sql(
      s"""
         |select * from lineitem where l_shipdate BETWEEN date'1993-01-01' AND date'1993-01-10'
         |""".stripMargin)

    sourceDF1.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath)

    val sqlStr =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price,
         |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
         |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         |    avg(l_quantity) AS avg_qty,
         |    avg(l_extendedprice) AS avg_price,
         |    avg(l_discount) AS avg_disc,
         |    count(*) AS count_order
         |FROM
         |    clickhouse.`$dataPath`
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runTPCHQueryBySQL(1, sqlStr, compareResult = false) {
      df =>
        val result = df.collect()
        assert(result.size == 4)
        assert(result(0).getString(0).equals("A"))
        assert(result(0).getString(1).equals("F"))
        assert(result(0).getDouble(2) == 3803858.0)

        assert(result(2).getString(0).equals("N"))
        assert(result(2).getString(1).equals("O"))
        assert(result(2).getDouble(2) == 7454519.0)

        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size == 1)

        val mergetreeScan = scanExec(0)
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))
        assert(mergetreeScan.metrics("numFiles").value == 3744)

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKeyOption
            .get
            .mkString(",")
            .equals("l_orderkey"))
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .primaryKeyOption
            .get
            .mkString(",")
            .equals("l_orderkey"))
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.size == 2)
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .partitionColumns(0)
            .equals("l_shipdate"))
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .partitionColumns(1)
            .equals("l_returnflag"))
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assert(addFiles.size == 3835)
        assert(addFiles.map(_.rows).sum == 602945)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1992-06-01")).size == 2)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1993-01-01")).size == 4)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1995-01-21")).size == 2)
    }
  }

  test("test mergetree path based write with bucket table") {
    val dataPath = s"$basePath/lineitem_mergetree_bucket"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .partitionBy("l_shipdate")
      .option("clickhouse.orderByKey", "l_partkey,l_returnflag")
      .option("clickhouse.primaryKey", "l_partkey")
      .option("clickhouse.numBuckets", "4")
      .option("clickhouse.bucketColumnNames", "l_orderkey")
      .mode(SaveMode.Append)
      .save(dataPath)

    val sqlStr =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price,
         |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
         |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         |    avg(l_quantity) AS avg_qty,
         |    avg(l_extendedprice) AS avg_price,
         |    avg(l_discount) AS avg_disc,
         |    count(*) AS count_order
         |FROM
         |    clickhouse.`$dataPath`
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runTPCHQueryBySQL(1, sqlStr) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size == 1)

        val mergetreeScan = scanExec(0)
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        val buckets = ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption
        assert(!buckets.isEmpty)
        assert(buckets.get.numBuckets == 4)
        assert(
          buckets.get.sortColumnNames
            .mkString(",")
            .equals("l_partkey,l_returnflag"))
        assert(
          buckets.get.bucketColumnNames
            .mkString(",")
            .equals("l_orderkey"))
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKeyOption
            .get
            .mkString(",")
            .equals("l_partkey,l_returnflag"))
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .primaryKeyOption
            .get
            .mkString(",")
            .equals("l_partkey"))
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.size == 1)
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .partitionColumns(0)
            .equals("l_shipdate"))
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assert(addFiles.size == 10089)
        assert(addFiles.map(_.rows).sum == 600572)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1992-06-01")).size == 4)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1993-01-01")).size == 4)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1995-01-21")).size == 4)
        assert(
          addFiles
            .filter(
              f =>
                f.partitionValues.get("l_shipdate").get.equals("1995-01-21") && f.bucketNum.equals(
                  "00000"))
            .size == 1)
    }
    // check part pruning effect of filter on bucket column
    val df = spark.sql(s"""
                          | select * from clickhouse.`$dataPath` where l_orderkey = 12647
                          | and l_shipdate = date'1997-06-02'
                          |""".stripMargin)
    df.collect()
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    val touchedParts = scanExec.head.getPartitions
      .flatMap(partition => partition.asInstanceOf[GlutenMergeTreePartition].partList)
      .map(_.name)
      .distinct
    assert(touchedParts.size == 1)

    // test upsert on partitioned & bucketed table
    upsertSourceTableAndCheck(dataPath)
  }

  test("test mergetree path based CTAS simple") {
    val dataPath = s"$basePath/lineitem_mergetree_ctas1"
    clearDataPath(dataPath)

    spark.sql(s"""
                 |CREATE TABLE clickhouse.`$dataPath`
                 |USING clickhouse
                 | as select * from lineitem
                 |""".stripMargin)

    val sqlStr =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price,
         |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
         |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         |    avg(l_quantity) AS avg_qty,
         |    avg(l_extendedprice) AS avg_price,
         |    avg(l_discount) AS avg_disc,
         |    count(*) AS count_order
         |FROM
         |    clickhouse.`$dataPath`
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runTPCHQueryBySQL(1, sqlStr) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size == 1)

        val mergetreeScan = scanExec(0)
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).orderByKeyOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).primaryKeyOption.isEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
        assert(addFiles.size == 6)
        assert(
          addFiles.map(_.rows).sum
            == 600572)
    }
  }

  test("test mergetree path based CTAS complex") {
    val dataPath = s"$basePath/lineitem_mergetree_ctas2"
    clearDataPath(dataPath)

    spark.sql(
      s"""
         |CREATE TABLE clickhouse.`$dataPath`
         |USING clickhouse
         |PARTITIONED BY (l_shipdate)
         |CLUSTERED BY (l_orderkey)
         |${if (sparkVersion.equals("3.2")) "" else "SORTED BY (l_partkey, l_returnflag)"} INTO 4 BUCKETS
         | as select * from lineitem
         |""".stripMargin)

    val sqlStr =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price,
         |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
         |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         |    avg(l_quantity) AS avg_qty,
         |    avg(l_extendedprice) AS avg_price,
         |    avg(l_discount) AS avg_disc,
         |    count(*) AS count_order
         |FROM
         |    clickhouse.`$dataPath`
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runTPCHQueryBySQL(1, sqlStr) { _ => {} }

  }

  test("test mergetree path based table with low cardinality column") {
    val dataPath = s"$basePath/lineitem_mergetree_lowcard"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .option("clickhouse.lowCardKey", "l_returnflag,L_LINESTATUS,l_quantity")
      .mode(SaveMode.Append)
      .save(dataPath)

    val sqlStr =
      s"""
         |SELECT
         |    l_returnflag,
         |    l_linestatus,
         |    sum(l_quantity) AS sum_qty,
         |    sum(l_extendedprice) AS sum_base_price,
         |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
         |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
         |    avg(l_quantity) AS avg_qty,
         |    avg(l_extendedprice) AS avg_price,
         |    avg(l_discount) AS avg_disc,
         |    count(*) AS count_order
         |FROM
         |    clickhouse.`$dataPath`
         |WHERE
         |    l_shipdate <= date'1998-09-02' - interval 1 day
         |GROUP BY
         |    l_returnflag,
         |    l_linestatus
         |ORDER BY
         |    l_returnflag,
         |    l_linestatus;
         |
         |""".stripMargin
    runTPCHQueryBySQL(1, sqlStr) { _ => {} }
    val directory = new File(dataPath)
    // find a folder whose name is like 48b70783-b3b8-4bf8-9c52-5261aead8e3e_0_006
    val partDir = directory.listFiles().filter(f => f.getName.length > 20).head
    val columnsFile = new File(partDir, "columns.txt")
    val columns = Source.fromFile(columnsFile).getLines().mkString
    assert(columns.contains("`l_returnflag` LowCardinality(Nullable(String))"))
    assert(columns.contains("`l_linestatus` LowCardinality(Nullable(String))"))

    // test low card column in measure
    val sqlStr2 =
      s"""
         |SELECT
         |  max(l_returnflag), min(l_quantity)
         |FROM
         |    clickhouse.`$dataPath`
         |GROUP BY
         |    l_linestatus
         |  order by l_linestatus
         |
         |""".stripMargin

    assert(
      // total rows should remain unchanged
      spark.sql(sqlStr2).collect().apply(0).get(0) == "R"
    )

    // test select *
    val sqlStr3 =
      s"""
         |SELECT
         |  *
         |FROM
         |    clickhouse.`$dataPath` limit 1
         |
         |""".stripMargin
    spark.sql(sqlStr3).collect()
  }

  test("test mergetree path based table with primary keys filter") {
    val dataPath = s"$basePath/lineitem_mergetree_orderbykey2"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .option("clickhouse.orderByKey", "l_shipdate,l_orderkey")
      .option("clickhouse.primaryKey", "l_shipdate")
      .mode(SaveMode.Append)
      .save(dataPath)

    val sqlStr =
      s"""
         |SELECT
         |    sum(l_extendedprice * l_discount) AS revenue
         |FROM
         |    clickhouse.`$dataPath`
         |WHERE
         |    l_shipdate >= date'1994-01-01'
         |    AND l_shipdate < date'1994-01-01' + interval 1 year
         |    AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
         |    AND l_quantity < 24
         |""".stripMargin
    runTPCHQueryBySQL(6, sqlStr) {
      df =>
        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size == 1)

        val mergetreeScan = scanExec(0)
        assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

        val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).clickhouseTableConfigs.nonEmpty)
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).bucketOption.isEmpty)
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .orderByKeyOption
            .get
            .mkString(",")
            .equals("l_shipdate,l_orderkey"))
        assert(
          ClickHouseTableV2
            .getTable(fileIndex.deltaLog)
            .primaryKeyOption
            .get
            .mkString(",")
            .equals("l_shipdate"))
        assert(ClickHouseTableV2.getTable(fileIndex.deltaLog).partitionColumns.isEmpty)
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])

        assert(addFiles.size == 6)
        assert(addFiles.map(_.rows).sum == 600572)

        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(plans.size == 1)
        assert(plans(0).metrics("selectedMarksPk").value === 17)
        assert(plans(0).metrics("totalMarksPk").value === 74)
    }
  }

  test("test simple minmax index") {
    val dataPath = s"$basePath/lineitem_mergetree_minmax"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .option("clickhouse.minmaxIndexKey", "l_receiptdate")
      .mode(SaveMode.Append)
      .save(dataPath)

    val df = spark
      .sql(s"""
              |select count(*) from clickhouse.`$dataPath` where l_receiptdate = '1998-12-27'
              |""".stripMargin)

    val ret = df.collect()
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(scanExec.size == 1)
    val mergetreeScan = scanExec(0)

    assert(ret.apply(0).get(0) == 1)
    val marks = mergetreeScan.metrics("selectedMarks").value
    assert(marks == 1)

    val directory = new File(dataPath)
    // find a folder whose name is like 48b70783-b3b8-4bf8-9c52-5261aead8e3e_0_006
    val partDir = directory.listFiles().filter(f => f.getName.length > 20).head
    assert(
      partDir.listFiles().exists(p => p.getName.contains("skp_idx__minmax_l_receiptdate.idx2")))
  }

  test("test simple bloom filter index") {
    val dataPath = s"$basePath/lineitem_mergetree_bf"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .option("clickhouse.bloomfilterIndexKey", "l_orderkey")
      .mode(SaveMode.Append)
      .save(dataPath)

    val df = spark
      .sql(s"""
              |select count(*) from clickhouse.`$dataPath` where l_orderkey = '600000'
              |""".stripMargin)
    val ret = df.collect()
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(scanExec.size == 1)
    val mergetreeScan = scanExec(0)
    assert(ret.apply(0).get(0) == 2)
    val marks = mergetreeScan.metrics("selectedMarks").value
    assert(marks == 1)

    val directory = new File(dataPath)
    // find a folder whose name is like 48b70783-b3b8-4bf8-9c52-5261aead8e3e_0_006
    val partDir = directory.listFiles().filter(f => f.getName.length > 20).head
    assert(
      partDir.listFiles().exists(p => p.getName.contains("skp_idx__bloomfilter_l_orderkey.idx")))
  }

  test("test simple set index") {
    val dataPath = s"$basePath/lineitem_mergetree_set"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .option("clickhouse.setIndexKey", "l_orderkey")
      .mode(SaveMode.Append)
      .save(dataPath)

    val df = spark
      .sql(s"""
              |select count(*) from clickhouse.`$dataPath` where l_orderkey = '600000'
              |""".stripMargin)

    val ret = df.collect()
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(scanExec.size == 1)
    val mergetreeScan = scanExec(0)
    assert(ret.apply(0).get(0) == 2)
    val marks = mergetreeScan.metrics("selectedMarks").value
    assert(marks == 1)

    val directory = new File(dataPath)
    // find a folder whose name is like 48b70783-b3b8-4bf8-9c52-5261aead8e3e_0_006
    val partDir = directory.listFiles().filter(f => f.getName.length > 20).head
    assert(partDir.listFiles().exists(p => p.getName.contains("skp_idx__set_l_orderkey.idx")))
  }

  test("GLUTEN-5219: Fix the table metadata sync issue for the CH backend") {
    def checkQueryResult(tableName: String): Unit = {
      val sqlStr =
        s"""
           |SELECT
           |    l_returnflag,
           |    l_linestatus,
           |    sum(l_quantity) AS sum_qty,
           |    sum(l_extendedprice) AS sum_base_price,
           |    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
           |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
           |    avg(l_quantity) AS avg_qty,
           |    avg(l_extendedprice) AS avg_price,
           |    avg(l_discount) AS avg_disc,
           |    count(*) AS count_order
           |FROM
           |    clickhouse.`$tableName`
           |WHERE
           |    l_shipdate <= date'1998-09-02' - interval 1 day
           |GROUP BY
           |    l_returnflag,
           |    l_linestatus
           |ORDER BY
           |    l_returnflag,
           |    l_linestatus;
           |
           |""".stripMargin
      runTPCHQueryBySQL(1, sqlStr) {
        df =>
          val scanExec = collect(df.queryExecution.executedPlan) {
            case f: FileSourceScanExecTransformer => f
          }
          assert(scanExec.size == 1)

          val mergetreeScan = scanExec(0)
          assert(mergetreeScan.nodeName.startsWith("Scan mergetree"))

          val fileIndex = mergetreeScan.relation.location.asInstanceOf[TahoeFileIndex]
          val addFiles =
            fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddMergeTreeParts])
          assert(addFiles.size == 6)
          assert(
            addFiles.map(_.rows).sum
              == 600572)
      }
    }

    val dataPath = s"$basePath/lineitem_mergetree_ctas_5219"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath)

    checkQueryResult(dataPath)

    val dataPath1 = s"$basePath/lineitem_mergetree_ctas_5219"
    clearDataPath(dataPath1)
    sourceDF.write
      .format("clickhouse")
      .mode(SaveMode.Append)
      .save(dataPath1)

    checkQueryResult(dataPath1)

    var dataPathFile = new File(dataPath1)
    assert(dataPathFile.isDirectory && dataPathFile.isDirectory)

    val fileFilter = new WildcardFileFilter("*_0_*")
    var dataFileList = dataPathFile.list(fileFilter)
    assert(dataFileList.size == 6)

    // re-create the same table
    val dataPath2 = s"$basePath/lineitem_mergetree_5219_s"
    for (i <- 0 until 10) {
      clearDataPath(dataPath2)

      sourceDF.write
        .format("clickhouse")
        .mode(SaveMode.Append)
        .save(dataPath2)

      checkQueryResult(dataPath2)
    }

    dataPathFile = new File(dataPath2)
    assert(dataPathFile.isDirectory && dataPathFile.isDirectory)

    dataFileList = dataPathFile.list(fileFilter)
    assert(dataFileList.size == 6)
  }
}
// scalastyle:off line.size.limit
