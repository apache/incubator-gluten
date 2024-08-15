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
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import io.delta.tables.DeltaTable

import java.io.File

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseDeltaParquetWriteSuite
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
      .set("spark.gluten.sql.native.writer.enabled", "true")
      .set("spark.sql.storeAssignmentPolicy", "legacy")
      .set(
        "spark.gluten.sql.columnar.backend.ch.runtime_settings.mergetree.merge_after_insert",
        "false")
      .set(
        "spark.databricks.delta.retentionDurationCheck.enabled",
        "false"
      )
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createNotNullTPCHTablesInParquet(tablesPath)
  }

  test("test parquet table write with the delta") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet
                 |(
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
                 |USING delta
                 |LOCATION '$basePath/lineitem_delta_parquet'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_delta_parquet
                 | select /*+ REPARTITION(5) */ * from lineitem
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
         |    lineitem_delta_parquet
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

        val parquetScan = plans(3).asInstanceOf[FileSourceScanExecTransformer]
        assert(parquetScan.nodeName.startsWith("Scan parquet "))

        val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddFile])
        assert(addFiles.size == 5)
    }
  }

  test("test parquet insert overwrite with the delta") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_insertoverwrite;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_insertoverwrite
                 |(
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
                 |USING delta
                 |LOCATION '$basePath/lineitem_delta_parquet_insertoverwrite'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_delta_parquet_insertoverwrite
                 | select * from lineitem
                 |""".stripMargin)

    spark.sql(s"""
                 | insert overwrite table lineitem_delta_parquet_insertoverwrite
                 | select * from lineitem where mod(l_orderkey,2) = 1
                 |""".stripMargin)
    val sql2 =
      s"""
         | select count(*) from lineitem_delta_parquet_insertoverwrite
         |""".stripMargin
    assert(
      // total rows should remain unchanged
      spark.sql(sql2).collect().apply(0).get(0) == 300001
    )
  }

  test("test parquet insert overwrite partitioned table with small table, static with delta") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_insertoverwrite2;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_insertoverwrite2
                 |(
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
                 |USING delta
                 |PARTITIONED BY (l_shipdate)
                 |LOCATION '$basePath/lineitem_delta_parquet_insertoverwrite2'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_delta_parquet_insertoverwrite2
                 | select * from lineitem
                 | where l_shipdate BETWEEN date'1993-01-01' AND date'1993-03-31'
                 |""".stripMargin)

    spark.sql(
      s"""
         | insert overwrite table lineitem_delta_parquet_insertoverwrite2
         | select * from lineitem where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
         |""".stripMargin)
    val sql2 =
      s"""
         | select count(*) from lineitem_delta_parquet_insertoverwrite2
         |
         |""".stripMargin
    assert(
      // total rows should remain unchanged
      spark.sql(sql2).collect().apply(0).get(0) == 2418
    )
  }

  test("test parquet insert overwrite partitioned table with small table, dynamic with delta") {
    withSQLConf(("spark.sql.sources.partitionOverwriteMode", "dynamic")) {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_delta_parquet_insertoverwrite3 PURGE;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_insertoverwrite3
                   |(
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
                   |USING delta
                   |PARTITIONED BY (l_shipdate)
                   |LOCATION '$basePath/lineitem_delta_parquet_insertoverwrite3'
                   |""".stripMargin)

      spark.sql(s"""
                   | insert into table lineitem_delta_parquet_insertoverwrite3
                   | select * from lineitem
                   | where l_shipdate BETWEEN date'1993-01-01' AND date'1993-03-31'
                   |""".stripMargin)

      spark.sql(
        s"""
           | insert overwrite table lineitem_delta_parquet_insertoverwrite3
           | select * from lineitem where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
           |""".stripMargin)
      val sql2 =
        s"""
           | select count(*) from lineitem_delta_parquet_insertoverwrite3
           |
           |""".stripMargin
      assert(
        // total rows should remain unchanged
        spark.sql(sql2).collect().apply(0).get(0) == 21875
      )
    }
  }

  test("test parquet table update with the delta") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_update;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_update
                 |(
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
                 |USING delta
                 |LOCATION '$basePath/lineitem_delta_parquet_update'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_delta_parquet_update
                 | select /*+ REPARTITION(6) */ * from lineitem
                 |""".stripMargin)

    spark.sql(
      s"""
         | update lineitem_delta_parquet_update set l_returnflag = 'Z' where l_orderkey = 12647
         |""".stripMargin)

    {
      val sql1 =
        s"""
           | select count(*) from lineitem_delta_parquet_update where l_returnflag = 'Z'
           |
           |""".stripMargin

      val df = spark.sql(sql1)
      val result = df.collect()
      assert(
        // in test data, there are only 1 row with l_orderkey = 12647
        result.apply(0).get(0) == 1
      )
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      assert(scanExec.size == 1)

      val parquetScan = scanExec.head
      assert(parquetScan.nodeName.startsWith("Scan parquet"))

      val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
      val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddFile])
      assert(addFiles.size == 4)
    }

    val sql2 =
      s"""
         | select count(*) from lineitem_delta_parquet_update
         |
         |""".stripMargin
    assert(
      // total rows should remain unchanged
      spark.sql(sql2).collect().apply(0).get(0) == 600572
    )
  }

  test("test parquet table delete with the delta") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_delete;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_delete
                 |(
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
                 |USING delta
                 |LOCATION '$basePath/lineitem_delta_parquet_delete'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_delta_parquet_delete
                 | select /*+ REPARTITION(6) */ * from lineitem
                 |""".stripMargin)
    val df1 = spark.sql(s"""
                           | delete from lineitem_delta_parquet_delete where l_orderkey = 12647
                           |""".stripMargin)

    {
      val df = spark.sql(s"""
                            | select sum(l_linenumber) from lineitem_delta_parquet_delete
                            |""".stripMargin)
      val result = df.collect()
      assert(
        result.apply(0).get(0) == 1802445
      )
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      val parquetScan = scanExec.head
      val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
      val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddFile])
      assert(addFiles.size == 4)
    }

    {
      spark.sql(s"""
                   | delete from lineitem_delta_parquet_delete where mod(l_orderkey, 3) = 2
                   |""".stripMargin)
      val df3 = spark.sql(s"""
                             | select sum(l_linenumber) from lineitem_delta_parquet_delete
                             |""".stripMargin)
      assert(
        df3.collect().apply(0).get(0) == 1200671
      )
    }
  }

  test("test parquet table upsert with the delta") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_upsert;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_upsert
                 |(
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
                 |USING delta
                 |LOCATION '$basePath/lineitem_delta_parquet_upsert'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_delta_parquet_upsert
                 | select * from lineitem
                 |""".stripMargin)

    {
      val df0 = spark.sql(s"""
                             | select sum(l_linenumber) from lineitem_delta_parquet_upsert
                             |""".stripMargin)
      assert(
        df0.collect().apply(0).get(0) == 1802446
      )
    }

    upsertSourceTableAndCheck("lineitem_delta_parquet_upsert")
  }

  private def upsertSourceTableAndCheck(tableName: String) = {
    // Why selecting l_orderkey having count(*) =1 ?
    // Answer: to avoid "org.apache.spark.sql.delta.DeltaUnsupportedOperationException:
    // Cannot perform Merge as multiple source rows matched and attempted to modify the same
    // target row in the Delta table in possibly conflicting ways."
    spark.sql(s"""
          merge into $tableName
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
          on updates.l_orderkey = $tableName.l_orderkey
          when matched then update set *
          when not matched then insert *
          """.stripMargin)

    {
      val df1 = spark.sql(s"""
                             | select count(*) from $tableName
                             |""".stripMargin)
      assert(
        df1.collect().apply(0).get(0) == 600572 + 3506
      )
    }
    {
      val df2 =
        spark.sql(s"""
                     | select count(*) from $tableName where l_returnflag = 'Z'
                     |""".stripMargin)
      assert(
        df2.collect().apply(0).get(0) == 3506
      )
    }

    {
      val df3 =
        spark.sql(s"""
                     | select count(*) from $tableName where l_orderkey > 10000000
                     |""".stripMargin)
      assert(
        df3.collect().apply(0).get(0) == 3506
      )
    }
  }

  test("test parquet write with partition + delta") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_partition;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_partition
                 |(
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
                 |USING delta
                 |PARTITIONED BY (l_shipdate, l_returnflag)
                 |LOCATION '$basePath/lineitem_delta_parquet_partition'
                 |""".stripMargin)

    // dynamic partitions
    spark.sql(s"""
                 | insert into table lineitem_delta_parquet_partition
                 | select * from lineitem
                 | where l_shipdate BETWEEN date'1993-01-01' AND date'1993-03-31'
                 |""".stripMargin)

    // write with dataframe api
    val source = spark.sql(s"""
                              |select
                              | l_orderkey      ,
                              | l_partkey       ,
                              | l_suppkey       ,
                              | l_linenumber    ,
                              | l_quantity      ,
                              | l_extendedprice ,
                              | l_discount      ,
                              | l_tax           ,
                              | l_returnflag    ,
                              | l_linestatus    ,
                              | l_shipdate      ,
                              | l_commitdate    ,
                              | l_receiptdate   ,
                              | l_shipinstruct  ,
                              | l_shipmode      ,
                              | l_comment
                              | from lineitem
                              | where l_shipdate BETWEEN date'1993-01-01' AND date'1993-01-10'
                              |""".stripMargin)

    source.write
      .format("delta")
      .mode(SaveMode.Append)
      .insertInto("lineitem_delta_parquet_partition")

    // static partition
    spark.sql(
      s"""
         | insert into lineitem_delta_parquet_partition PARTITION (l_shipdate=date'1993-02-21',
         | l_returnflag = 'A')
         | (l_orderkey,
         |  l_partkey,
         |  l_suppkey,
         |  l_linenumber,
         |  l_quantity,
         |  l_extendedprice,
         |  l_discount,
         |  l_tax,
         |  l_linestatus,
         |  l_commitdate,
         |  l_receiptdate,
         |  l_shipinstruct,
         |  l_shipmode,
         |  l_comment)
         | select l_orderkey,
         |  l_partkey,
         |  l_suppkey,
         |  l_linenumber,
         |  l_quantity,
         |  l_extendedprice,
         |  l_discount,
         |  l_tax,
         |  l_linestatus,
         |  l_commitdate,
         |  l_receiptdate,
         |  l_shipinstruct,
         |  l_shipmode,
         |  l_comment from lineitem
         |  where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
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
         |    lineitem_delta_parquet_partition
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
        assert(result.size == 2)
        assert(result(0).getString(0).equals("A"))
        assert(result(0).getString(1).equals("F"))
        assert(result(0).getDouble(2) == 368009.0)

        assert(result(1).getString(0).equals("R"))
        assert(result(1).getString(1).equals("F"))
        assert(result(1).getDouble(2) == 312371.0)

        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size == 1)

        val parquetScan = scanExec(0)
        assert(parquetScan.nodeName.startsWith("Scan parquet"))
        assert(parquetScan.metrics("numFiles").value == 201)

        val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddFile])

        assert(addFiles.size == 201)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1993-03-31")).size == 2)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1993-01-01")).size == 4)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1993-02-21")).size == 3)
    }
  }

  test("test parquet CTAS simple with the delta") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_ctas1;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE lineitem_delta_parquet_ctas1
                 |USING delta
                 |LOCATION '$basePath/lineitem_delta_parquet_ctas1'
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
         |    lineitem_delta_parquet_ctas1
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

        val parquetScan = scanExec(0)
        assert(parquetScan.nodeName.startsWith("Scan parquet"))

        val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddFile])
        assert(addFiles.size == 1)
    }
  }

  test("test parquet CTAS complex with the delta") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_ctas2;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_ctas2
                 |USING delta
                 |PARTITIONED BY (l_returnflag)
                 |LOCATION '$basePath/lineitem_mergetree_ctas2'
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
         |    lineitem_delta_parquet_ctas2
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

  test("test path based parquet write with the delta") {
    val dataPath = s"$basePath/lineitem_delta_parquet_filebased"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE delta.`$dataPath` (
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
                 |USING delta
                 |""".stripMargin)

    sourceDF.write
      .format("delta")
      .mode(SaveMode.Append)
      .save(dataPath)
    sourceDF.write
      .format("delta")
      .mode(SaveMode.Append)
      .save(dataPath)
    sourceDF.write
      .format("delta")
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
         |    delta.`$dataPath`
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

        val parquetScan = plans(3).asInstanceOf[FileSourceScanExecTransformer]
        assert(parquetScan.nodeName.startsWith("Scan parquet"))

        val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddFile])
        assert(addFiles.size == 1)
    }

    val result = spark.read
      .format("delta")
      .load(dataPath)
      .where("l_shipdate = date'1998-09-02'")
      .count()
    assert(result == 183)
  }

  test(
    "test path based parquet insert overwrite partitioned table with small table, static with delta") {
    val dataPath = s"$basePath/lineitem_delta_parquet_insertoverwrite2"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |where l_shipdate BETWEEN date'1993-01-01' AND date'1993-03-31'
                                |""".stripMargin)

    sourceDF.write
      .format("delta")
      .mode(SaveMode.Append)
      .partitionBy("l_shipdate")
      .save(dataPath)

    val sourceDF1 = spark.sql(
      s"""
         |select * from lineitem where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
         |""".stripMargin)
    sourceDF1.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .partitionBy("l_shipdate")
      .save(dataPath)

    val result = spark.read
      .format("delta")
      .load(dataPath)
      .count()
    assert(result == 2418)
  }

  test(
    "test path based parquet insert overwrite partitioned table with small table, dynamic with delta") {
    withSQLConf(("spark.sql.sources.partitionOverwriteMode", "dynamic")) {
      val dataPath = s"$basePath/lineitem_delta_parquet_insertoverwrite3"
      clearDataPath(dataPath)

      val sourceDF = spark.sql(s"""
                                  |select * from lineitem
                                  |where l_shipdate BETWEEN date'1993-01-01' AND date'1993-03-31'
                                  |""".stripMargin)

      sourceDF.write
        .format("delta")
        .mode(SaveMode.Append)
        .partitionBy("l_shipdate")
        .save(dataPath)

      val sourceDF1 = spark.sql(
        s"""
           |select * from lineitem where l_shipdate BETWEEN date'1993-02-01' AND date'1993-02-10'
           |""".stripMargin)
      sourceDF1.write
        .format("delta")
        .mode(SaveMode.Overwrite)
        .partitionBy("l_shipdate")
        .save(dataPath)

      val result = spark.read
        .format("delta")
        .load(dataPath)
        .count()
      assert(result == 21875)
    }
  }

  test("test path based parquet update with the delta") {
    val dataPath = s"$basePath/lineitem_delta_parquet_update"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select /*+ REPARTITION(6) */ * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("delta")
      .mode(SaveMode.Append)
      .save(dataPath)

    spark.sql(s"""
                 | update delta.`$dataPath` set l_returnflag = 'Z' where l_orderkey = 12647
                 |""".stripMargin)

    {
      val df = spark.read
        .format("delta")
        .load(dataPath)
        .where("l_returnflag = 'Z'")
      assert(df.count() == 1)
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      assert(scanExec.size == 1)

      val parquetScan = scanExec.head
      assert(parquetScan.nodeName.startsWith("Scan parquet"))

      val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
      val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddFile])
      assert(addFiles.size == 4)
    }

    val clickhouseTable = DeltaTable.forPath(spark, dataPath)
    clickhouseTable.updateExpr("l_orderkey = 10086", Map("l_returnflag" -> "'X'"))

    {
      val df = spark.read
        .format("delta")
        .load(dataPath)
        .where("l_returnflag = 'X'")
      assert(df.count() == 1)
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      assert(scanExec.size == 1)

      val parquetScan = scanExec.head
      assert(parquetScan.nodeName.startsWith("Scan parquet"))

      val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
      val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddFile])
      assert(addFiles.size == 3)
    }

    val df = spark.read
      .format("delta")
      .load(dataPath)
    assert(df.count() == 600572)
  }

  test("test path based parquet delete with the delta") {
    val dataPath = s"$basePath/lineitem_delta_parquet_delete"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select /*+ REPARTITION(6) */ * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("delta")
      .mode(SaveMode.Append)
      .save(dataPath)

    spark.sql(s"""
                 | delete from delta.`$dataPath` where l_orderkey = 12647
                 |""".stripMargin)
    val df = spark.read
      .format("delta")
      .load(dataPath)
    assert(df.count() == 600571)
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    val parquetScan = scanExec.head
    val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
    val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddFile])
    assert(addFiles.size == 4)

    val clickhouseTable = DeltaTable.forPath(spark, dataPath)
    clickhouseTable.delete("mod(l_orderkey, 3) = 2")
    val df1 = spark.read
      .format("delta")
      .load(dataPath)
    assert(df1.count() == 400089)
  }

  test("test path based parquet upsert with the delta") {
    val dataPath = s"$basePath/lineitem_delta_parquet_upsert"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select /*+ REPARTITION(6) */ * from lineitem
                                |""".stripMargin)

    sourceDF.write
      .format("delta")
      .mode(SaveMode.Append)
      .save(dataPath)

    val df0 = spark.sql(s"""
                           | select count(*) from delta.`$dataPath`
                           |""".stripMargin)
    assert(
      df0.collect().apply(0).get(0) == 600572
    )
    upsertPathBasedSourceTableAndCheck(dataPath)
  }

  private def upsertPathBasedSourceTableAndCheck(dataPath: String) = {
    // Why selecting l_orderkey having count(*) =1 ?
    // Answer: to avoid "org.apache.spark.sql.delta.DeltaUnsupportedOperationException:
    // Cannot perform Merge as multiple source rows matched and attempted to modify the same
    // target row in the Delta table in possibly conflicting ways."
    spark.sql(s"""
          merge into delta.`$dataPath`
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
          on updates.l_orderkey = delta.`$dataPath`.l_orderkey
          when matched then update set *
          when not matched then insert *
          """.stripMargin)

    {
      val df1 = spark.sql(s"""
                             | select count(*) from delta.`$dataPath`
                             |""".stripMargin)
      assert(
        df1.collect().apply(0).get(0) == 600572 + 3506
      )
    }
    {
      val df2 =
        spark.sql(s"""
                     | select count(*) from delta.`$dataPath` where l_returnflag = 'Z'
                     |""".stripMargin)
      assert(
        df2.collect().apply(0).get(0) == 3506
      )
    }

    {
      val df3 =
        spark.sql(s"""
                     | select count(*) from delta.`$dataPath` where l_orderkey > 10000000
                     |""".stripMargin)
      assert(
        df3.collect().apply(0).get(0) == 3506
      )
    }
  }

  test("test path based parquet write with partition + delta") {
    val dataPath = s"$basePath/lineitem_delta_parquet_partition"
    clearDataPath(dataPath)

    val sourceDF = spark.sql(s"""
                                |select * from lineitem
                                |where l_shipdate BETWEEN date'1993-01-01' AND date'1993-03-31'
                                |""".stripMargin)

    sourceDF.write
      .format("delta")
      .partitionBy("l_shipdate", "l_returnflag")
      .mode(SaveMode.Append)
      .save(dataPath)

    val sourceDF1 = spark.sql(
      s"""
         |select * from lineitem where l_shipdate BETWEEN date'1993-01-01' AND date'1993-01-10'
         |""".stripMargin)

    sourceDF1.write
      .format("delta")
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
         |    delta.`$dataPath`
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
        assert(result.size == 2)
        assert(result(0).getString(0).equals("A"))
        assert(result(0).getString(1).equals("F"))
        assert(result(0).getDouble(2) == 306633.0)

        assert(result(1).getString(0).equals("R"))
        assert(result(1).getString(1).equals("F"))
        assert(result(1).getDouble(2) == 312371.0)

        val scanExec = collect(df.queryExecution.executedPlan) {
          case f: FileSourceScanExecTransformer => f
        }
        assert(scanExec.size == 1)

        val parquetScan = scanExec(0)
        assert(parquetScan.nodeName.startsWith("Scan parquet"))
        assert(parquetScan.metrics("numFiles").value == 200)

        val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
        val addFiles = fileIndex.matchingFiles(Nil, Nil).map(f => f.asInstanceOf[AddFile])

        assert(addFiles.size == 200)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1993-03-31")).size == 2)
        assert(
          addFiles.filter(_.partitionValues.get("l_shipdate").get.equals("1993-01-01")).size == 4)
    }
  }

  test("test path based parquet CTAS with delta") {
    val dataPath = s"$basePath/lineitem_delta_parquet_ctas"
    clearDataPath(dataPath)

    spark.sql(s"""
                 |CREATE TABLE delta.`$dataPath`
                 |USING delta
                 |PARTITIONED BY (l_linestatus)
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
         |    delta.`$dataPath`
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

  testSparkVersionLE33("test parquet optimize basic") {
    withSQLConf("spark.databricks.delta.optimize.maxFileSize" -> "20000000") {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_delta_parquet_optimize;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_optimize
                   |USING delta
                   |LOCATION '$basePath/lineitem_delta_parquet_optimize'
                   | as select  /*+ REPARTITION(20) */ * from lineitem
                   |""".stripMargin)

      spark.sql("optimize lineitem_delta_parquet_optimize")
      val ret = spark.sql("select count(*) from lineitem_delta_parquet_optimize").collect()
      assert(ret.apply(0).get(0) == 600572)

      assert(
        countFiles(new File(s"$basePath/lineitem_delta_parquet_optimize")) == 24
      )
    }
  }

  def countFiles(directory: File): Int = {
    if (directory.exists && directory.isDirectory && !directory.getName.equals("_commits")) {
      val files = directory.listFiles
      val count = files
        .filter(!_.getName.endsWith(".crc"))
        .count(_.isFile) + files.filter(_.isDirectory).map(countFiles).sum
      count
    } else {
      0
    }
  }

  testSparkVersionLE33("test parquet optimize partitioned by one low card column") {
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_optimize_p2;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_optimize_p2
                 |USING delta
                 |PARTITIONED BY (l_returnflag)
                 |LOCATION '$basePath/lineitem_delta_parquet_optimize_p2'
                 | as select /*+ REPARTITION(6) */ * from lineitem
                 |""".stripMargin)

    spark.sparkContext.setJobGroup("test3", "test3")
    spark.sql("optimize lineitem_delta_parquet_optimize_p2")
    val job_ids = spark.sparkContext.statusTracker.getJobIdsForGroup("test3")
    if (sparkVersion.equals("3.2")) {
      assert(job_ids.size == 7) // WILL trigger actual merge job
    } else {
      assert(job_ids.size == 8) // WILL trigger actual merge job
    }

    spark.sparkContext.clearJobGroup()

    val ret = spark.sql("select count(*) from lineitem_delta_parquet_optimize_p2").collect()
    assert(ret.apply(0).get(0) == 600572)

    assert(countFiles(new File(s"$basePath/lineitem_delta_parquet_optimize_p2")) == 23)
    spark.sql("VACUUM lineitem_delta_parquet_optimize_p2 RETAIN 0 HOURS")
    if (sparkVersion.equals("3.2")) {
      assert(countFiles(new File(s"$basePath/lineitem_delta_parquet_optimize_p2")) == 5)
    } else {
      assert(countFiles(new File(s"$basePath/lineitem_delta_parquet_optimize_p2")) == 7)
    }

    val ret2 = spark.sql("select count(*) from lineitem_delta_parquet_optimize_p2").collect()
    assert(ret2.apply(0).get(0) == 600572)
  }

  testSparkVersionLE33("test parquet optimize parallel delete") {
    withSQLConf("spark.databricks.delta.vacuum.parallelDelete.enabled" -> "true") {
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_delta_parquet_optimize_p4;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_optimize_p4
                   |USING delta
                   |PARTITIONED BY (l_linenumber,l_returnflag)
                   |LOCATION '$basePath/lineitem_delta_parquet_optimize_p4'
                   | as select /*+ REPARTITION(6) */ * from lineitem
                   |""".stripMargin)

      spark.sql("optimize lineitem_delta_parquet_optimize_p4")
      val ret = spark.sql("select count(*) from lineitem_delta_parquet_optimize_p4").collect()
      assert(ret.apply(0).get(0) == 600572)

      assert(countFiles(new File(s"$basePath/lineitem_delta_parquet_optimize_p4")) == 149)
      spark.sql("VACUUM lineitem_delta_parquet_optimize_p4 RETAIN 0 HOURS")
      if (sparkVersion.equals("3.2")) {
        assert(countFiles(new File(s"$basePath/lineitem_delta_parquet_optimize_p4")) == 23)
      } else {
        assert(countFiles(new File(s"$basePath/lineitem_delta_parquet_optimize_p4")) == 25)
      }

      val ret2 = spark.sql("select count(*) from lineitem_delta_parquet_optimize_p4").collect()
      assert(ret2.apply(0).get(0) == 600572)
    }
  }

  testSparkVersionLE33("test parquet optimize with the path based table") {
    val dataPath = s"$basePath/lineitem_delta_parquet_optimize_path_based"
    clearDataPath(dataPath)
    withSQLConf(
      ("spark.databricks.delta.optimize.maxFileSize" -> "1000000"),
      ("spark.databricks.delta.optimize.minFileSize" -> "838000")) {

      val sourceDF = spark.sql(s"""
                                  |select /*+ REPARTITION(50) */ * from lineitem
                                  |""".stripMargin)

      sourceDF.write
        .format("delta")
        .mode(SaveMode.Append)
        .save(dataPath)

      assert(countFiles(new File(dataPath)) === 51)

      val clickhouseTable = DeltaTable.forPath(spark, dataPath)
      clickhouseTable.optimize().executeCompaction()

      clickhouseTable.vacuum(0.0)
      if (sparkVersion.equals("3.2")) {
        assert(countFiles(new File(dataPath)) === 27)
      } else {
        assert(countFiles(new File(dataPath)) === 29)
      }

      val ret = spark.sql(s"select count(*) from clickhouse.`$dataPath`").collect()
      assert(ret.apply(0).get(0) == 600572)
    }

    withSQLConf(
      ("spark.databricks.delta.optimize.maxFileSize" -> "10000000"),
      ("spark.databricks.delta.optimize.minFileSize" -> "1000000")) {

      val clickhouseTable = DeltaTable.forPath(spark, dataPath)
      clickhouseTable.optimize().executeCompaction()

      clickhouseTable.vacuum(0.0)
      if (sparkVersion.equals("3.2")) {
        assert(countFiles(new File(dataPath)) == 6)
      } else {
        assert(countFiles(new File(dataPath)) == 12)
      }

      val ret = spark.sql(s"select count(*) from clickhouse.`$dataPath`").collect()
      assert(ret.apply(0).get(0) == 600572)
    }

    // now merge all parts (testing merging from merged parts)
    val clickhouseTable = DeltaTable.forPath(spark, dataPath)
    clickhouseTable.optimize().executeCompaction()

    clickhouseTable.vacuum(0.0)
    if (sparkVersion.equals("3.2")) {
      assert(countFiles(new File(dataPath)) == 5)
    } else {
      assert(countFiles(new File(dataPath)) == 13)
    }

    val ret = spark.sql(s"select count(*) from clickhouse.`$dataPath`").collect()
    assert(ret.apply(0).get(0) == 600572)
  }
}
// scalastyle:off line.size.limit
