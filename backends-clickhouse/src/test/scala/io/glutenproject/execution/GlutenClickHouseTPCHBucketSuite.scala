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

import org.apache.spark.{SPARK_VERSION_SHORT, SparkConf}
import org.apache.spark.sql.execution.InputIteratorTransformer
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.commons.io.FileUtils

import java.io.File

// Some sqls' line length exceeds 100
// scalastyle:off line.size.limit

class GlutenClickHouseTPCHBucketSuite
  extends GlutenClickHouseTPCHAbstractSuite
  with AdaptiveSparkPlanHelper {

  override protected val tablesPath: String = basePath + "/tpch-data-ch"
  override protected val tpchQueries: String = rootPath + "queries/tpch-queries-ch"
  override protected val queriesResults: String = rootPath + "bucket-queries-output"

  protected lazy val sparkVersion: String = {
    val version = SPARK_VERSION_SHORT.split("\\.")
    version(0) + "." + version(1)
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.io.compression.codec", "LZ4")
      .set("spark.sql.shuffle.partitions", "5")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1") // for test bucket join
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.gluten.sql.columnar.backend.ch.shuffle.hash.algorithm", "sparkMurmurHash3_32")
  }

  override protected val createNullableTables = true

  override protected def createTPCHNullableTables(): Unit = {
    // create parquet data source table
    val parquetSourceDB = "parquet_source"
    spark.sql(s"""
                 |CREATE DATABASE IF NOT EXISTS $parquetSourceDB
                 |""".stripMargin)
    spark.sql(s"use $parquetSourceDB")

    val parquetTablePath = basePath + "/tpch-data"
    val parquetTableDataPath: String =
      "../../../../gluten-core/src/test/resources/tpch-data"
    FileUtils.copyDirectory(new File(rootPath + parquetTableDataPath), new File(parquetTablePath))

    createTPCHParquetTables(parquetTablePath)

    spark.sql(s"""
                 |CREATE DATABASE IF NOT EXISTS tpch_mergetree_bucket
                 |""".stripMargin)
    spark.sql("use tpch_mergetree_bucket")
    val customerData = tablesPath + "/customer"
    spark.sql(s"DROP TABLE IF EXISTS customer")
    // On Spark 3.2, bucket table does not support to create bucket column with sort columns for
    // DS V2
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS customer (
                 | c_custkey    bigint,
                 | c_name       string,
                 | c_address    string,
                 | c_nationkey  bigint,
                 | c_phone      string,
                 | c_acctbal    double,
                 | c_mktsegment string,
                 | c_comment    string)
                 | USING clickhouse
                 | LOCATION '$customerData'
                 | CLUSTERED BY (c_custkey)
                 | ${if (sparkVersion.equals("3.2")) "" else "SORTED BY (c_custkey)"} INTO 2 BUCKETS;
                 |""".stripMargin)

    val lineitemData = tablesPath + "/lineitem"
    spark.sql(s"DROP TABLE IF EXISTS lineitem")
    spark.sql(
      s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS lineitem (
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
         | l_comment       string)
         | USING clickhouse
         | LOCATION '$lineitemData'
         | CLUSTERED BY (l_orderkey)
         | ${if (sparkVersion.equals("3.2")) "" else "SORTED BY (l_shipdate, l_orderkey)"} INTO 2 BUCKETS;
         |""".stripMargin)

    val nationData = tablesPath + "/nation"
    spark.sql(s"DROP TABLE IF EXISTS nation")
    spark.sql(
      s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS nation (
         | n_nationkey bigint,
         | n_name      string,
         | n_regionkey bigint,
         | n_comment   string)
         | USING clickhouse
         | LOCATION '$nationData'
         | CLUSTERED BY (n_nationkey)
         | ${if (sparkVersion.equals("3.2")) "" else "SORTED BY (n_nationkey)"} INTO 1 BUCKETS;
         |""".stripMargin)

    val regionData = tablesPath + "/region"
    spark.sql(s"DROP TABLE IF EXISTS region")
    spark.sql(
      s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS region (
         | r_regionkey bigint,
         | r_name      string,
         | r_comment   string)
         | USING clickhouse
         | LOCATION '$regionData'
         | CLUSTERED BY (r_regionkey)
         | ${if (sparkVersion.equals("3.2")) "" else "SORTED BY (r_regionkey)"} INTO 1 BUCKETS;
         |""".stripMargin)

    val ordersData = tablesPath + "/orders"
    spark.sql(s"DROP TABLE IF EXISTS orders")
    spark.sql(
      s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS orders (
         | o_orderkey      bigint,
         | o_custkey       bigint,
         | o_orderstatus   string,
         | o_totalprice    double,
         | o_orderdate     date,
         | o_orderpriority string,
         | o_clerk         string,
         | o_shippriority  bigint,
         | o_comment       string)
         | USING clickhouse
         | LOCATION '$ordersData'
         | CLUSTERED BY (o_orderkey)
         | ${if (sparkVersion.equals("3.2")) "" else "SORTED BY (o_orderkey, o_orderdate)"} INTO 2 BUCKETS;
         |""".stripMargin)

    val partData = tablesPath + "/part"
    spark.sql(s"DROP TABLE IF EXISTS part")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS part (
                 | p_partkey     bigint,
                 | p_name        string,
                 | p_mfgr        string,
                 | p_brand       string,
                 | p_type        string,
                 | p_size        bigint,
                 | p_container   string,
                 | p_retailprice double,
                 | p_comment     string)
                 | USING clickhouse
                 | LOCATION '$partData'
                 | CLUSTERED BY (p_partkey)
                 | ${if (sparkVersion.equals("3.2")) "" else "SORTED BY (p_partkey)"} INTO 2 BUCKETS;
                 |""".stripMargin)

    val partsuppData = tablesPath + "/partsupp"
    spark.sql(s"DROP TABLE IF EXISTS partsupp")
    spark.sql(
      s"""
         | CREATE EXTERNAL TABLE IF NOT EXISTS partsupp (
         | ps_partkey    bigint,
         | ps_suppkey    bigint,
         | ps_availqty   bigint,
         | ps_supplycost double,
         | ps_comment    string)
         | USING clickhouse
         | LOCATION '$partsuppData'
         | CLUSTERED BY (ps_partkey)
         | ${if (sparkVersion.equals("3.2")) "" else "SORTED BY (ps_partkey)"} INTO 2 BUCKETS;
         |""".stripMargin)

    val supplierData = tablesPath + "/supplier"
    spark.sql(s"DROP TABLE IF EXISTS supplier")
    spark.sql(s"""
                 | CREATE EXTERNAL TABLE IF NOT EXISTS supplier (
                 | s_suppkey   bigint,
                 | s_name      string,
                 | s_address   string,
                 | s_nationkey bigint,
                 | s_phone     string,
                 | s_acctbal   double,
                 | s_comment   string)
                 | USING clickhouse
                 | LOCATION '$supplierData'
                 | CLUSTERED BY (s_suppkey)
                 | ${if (sparkVersion.equals("3.2")) "" else "SORTED BY (s_suppkey)"} INTO 1 BUCKETS;
                 |""".stripMargin)

    val result = spark
      .sql(s"""
              | show tables;
              |""".stripMargin)
      .collect()
    assert(result.length == 8)

    insertIntoMergeTreeTPCHTables(parquetSourceDB)
  }

  test("TPCH Q1") {
    runTPCHQuery(1)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(!(plans(0).asInstanceOf[FileSourceScanExecTransformer].bucketedScan))
        assert(plans(0).metrics("numFiles").value === 2)
        assert(plans(0).metrics("pruningTime").value === -1)
        assert(plans(0).metrics("outputRows").value === 591673)
      })
  }

  test("TPCH Q2") {
    runTPCHQuery(2)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[InputIteratorTransformer])
        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[InputIteratorTransformer])

        // Check the bucket join
        assert(
          plans(4)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[ProjectExecTransformer])
        assert(
          plans(4)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[FilterExecTransformerBase])

        assert(
          plans(9)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[InputIteratorTransformer])
        if (sparkVersion.equals("3.2")) {
          assert(
            plans(9)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .right
              .isInstanceOf[InputIteratorTransformer])
        } else {
          assert(
            plans(9)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .right
              .isInstanceOf[FilterExecTransformerBase])
        }

        if (sparkVersion.equals("3.2")) {
          assert(!(plans(11).asInstanceOf[FileSourceScanExecTransformer].bucketedScan))
        } else {
          assert(plans(11).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        }
        assert(plans(11).metrics("numFiles").value === 1)
        assert(plans(11).metrics("outputRows").value === 1000)
      })
  }

  test("TPCH Q3") {
    runTPCHQuery(3)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        if (sparkVersion.equals("3.2")) {
          assert(
            plans(1)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .left
              .isInstanceOf[InputIteratorTransformer])
        } else {
          assert(
            plans(1)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .left
              .isInstanceOf[ProjectExecTransformer])
        }
        assert(
          plans(1)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[InputIteratorTransformer])

        if (sparkVersion.equals("3.2")) {
          assert(!(plans(2).asInstanceOf[FileSourceScanExecTransformer].bucketedScan))
        } else {
          assert(plans(2).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        }
        assert(plans(2).metrics("numFiles").value === 2)
        assert(plans(2).metrics("outputRows").value === 3111)

        assert(!(plans(3).asInstanceOf[FileSourceScanExecTransformer].bucketedScan))
        assert(plans(3).metrics("numFiles").value === 2)
        assert(plans(3).metrics("outputRows").value === 72678)
      })
  }

  test("TPCH Q4") {
    runTPCHQuery(4)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        // bucket join
        assert(
          plans(0)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[ProjectExecTransformer])
        assert(
          plans(0)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[ProjectExecTransformer])

        assert(plans(1).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(1).metrics("numFiles").value === 2)
        assert(plans(1).metrics("outputRows").value === 5552)

        assert(plans(2).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(2).metrics("numFiles").value === 2)
        assert(plans(2).metrics("outputRows").value === 379809)
      })
  }

  test("TPCH Q6") {
    runTPCHQuery(6)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
        }
        assert(!(plans(0).asInstanceOf[FileSourceScanExecTransformer].bucketedScan))
        assert(plans(0).metrics("numFiles").value === 2)
        assert(plans(0).metrics("pruningTime").value === -1)
        assert(plans(0).metrics("outputRows").value === 11618)
      })
  }

  test("TPCH Q12") {
    runTPCHQuery(12)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case scanExec: BasicScanExecTransformer => scanExec
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        // bucket join
        assert(
          plans(0)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[FilterExecTransformerBase])
        assert(
          plans(0)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[ProjectExecTransformer])

        assert(plans(1).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(1).metrics("numFiles").value === 2)
        assert(plans(1).metrics("outputRows").value === 150000)

        assert(plans(2).asInstanceOf[FileSourceScanExecTransformer].bucketedScan)
        assert(plans(2).metrics("numFiles").value === 2)
        assert(plans(2).metrics("outputRows").value === 3155)
      })
  }

  test("TPCH Q18") {
    runTPCHQuery(18)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        // bucket join
        assert(
          plans(2)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[FilterExecTransformerBase])
        assert(
          plans(2)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[ProjectExecTransformer])
        // bucket join
        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[FilterExecTransformerBase])
        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[ProjectExecTransformer])
      })
  }

  test("TPCH Q20") {
    runTPCHQuery(20)(
      df => {
        val plans = collect(df.queryExecution.executedPlan) {
          case joinExec: HashJoinLikeExecTransformer => joinExec
        }
        if (sparkVersion.equals("3.2")) {
          assert(
            plans(1)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .left
              .isInstanceOf[InputIteratorTransformer])
        } else {
          assert(
            plans(1)
              .asInstanceOf[HashJoinLikeExecTransformer]
              .left
              .isInstanceOf[FilterExecTransformerBase])
        }
        assert(
          plans(1)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[InputIteratorTransformer])

        assert(
          plans(2)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[InputIteratorTransformer])
        assert(
          plans(2)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[InputIteratorTransformer])

        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .left
            .isInstanceOf[FilterExecTransformerBase])
        assert(
          plans(3)
            .asInstanceOf[HashJoinLikeExecTransformer]
            .right
            .isInstanceOf[ProjectExecTransformer])
      })
  }
}
// scalastyle:off line.size.limit
