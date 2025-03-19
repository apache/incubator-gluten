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
package org.apache.spark.gluten.delta

import org.apache.gluten.execution.{FileSourceScanExecTransformer, GlutenClickHouseTPCHAbstractSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.files.TahoeFileIndex
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenDeltaParquetDeletionVectorSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val needCopyParquetToTablePath = true

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "mergetree-queries-output"

  // import org.apache.gluten.backendsapi.clickhouse.CHConfig._

  /** Run Gluten + ClickHouse Backend with SortShuffleManager */
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "10MB")
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.files.maxPartitionBytes", "20000000")
      .set("spark.sql.storeAssignmentPolicy", "legacy")
      // .setCHConfig("use_local_format", true)
      .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
  }

  override protected def createTPCHNotNullTables(): Unit = {
    createNotNullTPCHTablesInParquet(tablesPath)
  }

  private val q1SchemaString: String =
    s""" l_orderkey      bigint,
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
       | l_comment       string""".stripMargin

  test("test parquet table delete with the delta DV") {
    spark.sql(s"""
                 |set spark.gluten.enabled=false;
                 |""".stripMargin)
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_delete_dv;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_delete_dv
                 |($q1SchemaString)
                 |USING delta
                 |TBLPROPERTIES (delta.enableDeletionVectors='true')
                 |LOCATION '$basePath/lineitem_delta_parquet_delete_dv'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_delta_parquet_delete_dv
                 | select /*+ REPARTITION(6) */ * from lineitem
                 |""".stripMargin)

    spark.sql(s"""
                 | delete from lineitem_delta_parquet_delete_dv
                 | where l_orderkey = 3
                 |""".stripMargin)

    spark.sql(s"""
                 |set spark.gluten.enabled=true;
                 |""".stripMargin)
    val df = spark.sql(s"""
                          | select sum(l_linenumber) from lineitem_delta_parquet_delete_dv
                          |""".stripMargin)
    val result = df.collect()
    assert(
      result.apply(0).get(0) === 1802425
    )
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    val parquetScan = scanExec.head
    val fileIndex = parquetScan.relation.location.asInstanceOf[TahoeFileIndex]
    val addFiles = fileIndex.matchingFiles(Nil, Nil)
    assert(addFiles.size === 6)

    spark.sql(s"""
                 |set spark.gluten.enabled=false;
                 |""".stripMargin)
    spark.sql(s"""
                 | delete from lineitem_delta_parquet_delete_dv where mod(l_orderkey, 3) = 2
                 |""".stripMargin)

    spark.sql(s"""
                 |set spark.gluten.enabled=true;
                 |""".stripMargin)
    val df3 = spark.sql(s"""
                           | select sum(l_linenumber) from lineitem_delta_parquet_delete_dv
                           |""".stripMargin)
    assert(
      df3.collect().apply(0).get(0) === 1200650
    )
  }

  test("test parquet table delete + update with the delta DV") {
    spark.sql(s"""
                 |set spark.gluten.enabled=false;
                 |""".stripMargin)
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_update_dv;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_update_dv
                 |($q1SchemaString)
                 |USING delta
                 |TBLPROPERTIES (delta.enableDeletionVectors='true')
                 |LOCATION '$basePath/lineitem_delta_parquet_update_dv'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_delta_parquet_update_dv
                 | select * from lineitem
                 |""".stripMargin)

    spark.sql(
      s"""
         | update lineitem_delta_parquet_update_dv set l_returnflag = 'AAA' where l_orderkey < 200
         |""".stripMargin)

    spark.sql(s"""
                 |set spark.gluten.enabled=true;
                 |""".stripMargin)
    val df =
      spark.sql(s"""
                   | select sum(l_linenumber)
                   | from lineitem_delta_parquet_update_dv
                   | where l_returnflag = 'AAA'
                   |""".stripMargin)
    val result = df.collect()
    assert(
      result.apply(0).get(0) === 662
    )
    val scanExec = collect(df.queryExecution.executedPlan) {
      case f: FileSourceScanExecTransformer => f
    }
    assert(scanExec.nonEmpty)
  }

  test("test parquet partition table delete with the delta DV") {
    withSQLConf(("spark.sql.sources.partitionOverwriteMode", "dynamic")) {
      spark.sql(s"""
                   |set spark.gluten.enabled=false;
                   |""".stripMargin)
      spark.sql(s"""
                   |DROP TABLE IF EXISTS lineitem_delta_partition_parquet_delete_dv;
                   |""".stripMargin)

      spark.sql(s"""
                   |CREATE TABLE IF NOT EXISTS lineitem_delta_partition_parquet_delete_dv
                   |($q1SchemaString)
                   |USING delta
                   |PARTITIONED BY (l_returnflag)
                   |TBLPROPERTIES (delta.enableDeletionVectors='true')
                   |LOCATION '$basePath/lineitem_delta_partition_parquet_delete_dv'
                   |""".stripMargin)

      spark.sql(s"""
                   | insert into table lineitem_delta_partition_parquet_delete_dv
                   | select /*+ REPARTITION(6) */ * from lineitem
                   |""".stripMargin)

      spark.sql(s"""
                   | delete from lineitem_delta_partition_parquet_delete_dv
                   | where mod(l_orderkey, 3) = 1
                   |""".stripMargin)

      spark.sql(s"""
                   |set spark.gluten.enabled=true;
                   |""".stripMargin)
      val df =
        spark.sql(s"""
                     | select sum(l_linenumber) from lineitem_delta_partition_parquet_delete_dv
                     |""".stripMargin)
      val result = df.collect()
      assert(
        result.apply(0).get(0) === 1201486
      )
      val scanExec = collect(df.queryExecution.executedPlan) {
        case f: FileSourceScanExecTransformer => f
      }
      assert(scanExec.nonEmpty)
    }
  }

  test("test parquet table upsert with the delta DV") {
    spark.sql(s"""
                 |set spark.gluten.enabled=false;
                 |""".stripMargin)
    spark.sql(s"""
                 |DROP TABLE IF EXISTS lineitem_delta_parquet_upsert_dv;
                 |""".stripMargin)

    spark.sql(s"""
                 |CREATE TABLE IF NOT EXISTS lineitem_delta_parquet_upsert_dv
                 |($q1SchemaString)
                 |USING delta
                 |TBLPROPERTIES (delta.enableDeletionVectors='true')
                 |LOCATION '$basePath/lineitem_delta_parquet_upsert_dv'
                 |""".stripMargin)

    spark.sql(s"""
                 | insert into table lineitem_delta_parquet_upsert_dv
                 | select * from lineitem
                 |""".stripMargin)

    spark.sql(s"""
                 |set spark.gluten.enabled=true;
                 |""".stripMargin)
    val df0 = spark.sql(s"""
                           | select sum(l_linenumber) from lineitem_delta_parquet_upsert_dv
                           |""".stripMargin)
    assert(
      df0.collect().apply(0).get(0) === 1802446
    )
    upsertSourceTableAndCheck("lineitem_delta_parquet_upsert_dv")
  }

  private def upsertSourceTableAndCheck(tableName: String) = {
    spark.sql(s"""
                 |set spark.gluten.enabled=false;
                 |""".stripMargin)
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

    spark.sql(s"""
                 |set spark.gluten.enabled=true;
                 |""".stripMargin)
    val df1 = spark.sql(s"""
                           | select sum(l_linenumber) from $tableName
                           |""".stripMargin)
    assert(
      df1.collect().apply(0).get(0) === 1805952
    )

    val df2 =
      spark.sql(s"""
                   | select count(*) from $tableName where l_returnflag = 'Z'
                   |""".stripMargin)
    assert(
      df2.collect().apply(0).get(0) === 3506
    )

    val df3 =
      spark.sql(s"""
                   | select count(*) from $tableName where l_orderkey > 10000000
                   |""".stripMargin)
    assert(
      df3.collect().apply(0).get(0) === 3506
    )
  }
}
// scalastyle:off line.size.limit
