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

package io.glutenproject.benchmarks

object GenTPCHTableScripts {

  def main(args: Array[String]): Unit = {
    genTPCHCSV2ParquetSQL()
    genTPCHMergeTreeTables()
    genTPCHParquetTables()
  }

  def genTPCHMergeTreeTables(): Unit = {
    // scalastyle:off println
    val dbName = "chdb"
    val dataPathRoot = "/data1/gluten_jni_db/gluten-tables/mergetree/"

    val tablePrefix = "chdb."
    val tableSuffix = "1000"

    val customerTbl = "customer"
    val lineitemTbl = "lineitem"
    val nationTbl = "nation"
    val regionTbl = "region"
    val ordersTbl = "orders"
    val ordersPath = "order"
    val partTbl = "part"
    val partsuppTbl = "partsupp"
    val supplierTbl = "supplier"

    println(
      s"""
         |CREATE DATABASE IF NOT EXISTS ${dbName}
         |WITH DBPROPERTIES (engine='MergeTree');
         |""".stripMargin)
    println(s"""use ${dbName};""")
    println("")

    // customer
    println(
      s"""
         |DROP TABLE IF EXISTS ${tablePrefix}${customerTbl}${tableSuffix};
         |""".stripMargin)
    println(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}${customerTbl}${tableSuffix} (
         | c_custkey    bigint,
         | c_name       string,
         | c_address    string,
         | c_nationkey  bigint,
         | c_phone      string,
         | c_acctbal    double,
         | c_mktsegment string,
         | c_comment    string)
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION 'file://${dataPathRoot + customerTbl}';
         |""".stripMargin)
    println("")
    println("")

    // lineitem
    println(
      s"""
         |DROP TABLE IF EXISTS ${tablePrefix}${lineitemTbl}${tableSuffix};
         |""".stripMargin)
    println(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}${lineitemTbl}${tableSuffix} (
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
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION 'file://${dataPathRoot + lineitemTbl}';
         |""".stripMargin)
    println("")
    println("")

    // nation
    println(
      s"""
         |DROP TABLE IF EXISTS ${tablePrefix}${nationTbl}${tableSuffix};
         |""".stripMargin)
    println(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}${nationTbl}${tableSuffix} (
         | n_nationkey bigint,
         | n_name      string,
         | n_regionkey bigint,
         | n_comment   string)
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION 'file://${dataPathRoot + nationTbl}';
         |""".stripMargin)
    println("")
    println("")

    // region
    println(
      s"""
         |DROP TABLE IF EXISTS ${tablePrefix}${regionTbl}${tableSuffix};
         |""".stripMargin)
    println(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}${regionTbl}${tableSuffix} (
         | r_regionkey bigint,
         | r_name      string,
         | r_comment   string)
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION 'file://${dataPathRoot + regionTbl}';
         |""".stripMargin)
    println("")
    println("")

    // orders
    println(
      s"""
         |DROP TABLE IF EXISTS ${tablePrefix}${ordersTbl}${tableSuffix};
         |""".stripMargin)
    println(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}${ordersTbl}${tableSuffix} (
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
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION 'file://${dataPathRoot + ordersPath}';
         |""".stripMargin)
    println("")
    println("")

    // part
    println(
      s"""
         |DROP TABLE IF EXISTS ${tablePrefix}${partTbl}${tableSuffix};
         |""".stripMargin)
    println(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}${partTbl}${tableSuffix} (
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
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION 'file://${dataPathRoot + partTbl}';
         |""".stripMargin)
    println("")
    println("")

    // partsupp
    println(
      s"""
         |DROP TABLE IF EXISTS ${tablePrefix}${partsuppTbl}${tableSuffix};
         |""".stripMargin)
    println(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}${partsuppTbl}${tableSuffix} (
         | ps_partkey    bigint,
         | ps_suppkey    bigint,
         | ps_availqty   bigint,
         | ps_supplycost double,
         | ps_comment    string)
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION 'file://${dataPathRoot + partsuppTbl}';
         |""".stripMargin)
    println("")
    println("")

    // supplier
    println(
      s"""
         |DROP TABLE IF EXISTS ${tablePrefix}${supplierTbl}${tableSuffix};
         |""".stripMargin)
    println(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}${supplierTbl}${tableSuffix} (
         | s_suppkey   bigint,
         | s_name      string,
         | s_address   string,
         | s_nationkey bigint,
         | s_phone     string,
         | s_acctbal   double,
         | s_comment   string)
         | USING clickhouse
         | TBLPROPERTIES (engine='MergeTree'
         |                )
         | LOCATION 'file://${dataPathRoot + supplierTbl}';
         |""".stripMargin)
    println("")
    println("")
    // scalastyle:on println
  }

  def genTPCHParquetTables(): Unit = {
    // scalastyle:off println
    val dbName = "tpchdb"
    val dataPathRoot = "/data1/gluten_jni_db/gluten-tables/parquets/"

    val tablePrefix = "tpchdb."
    val tableSuffix = "1000"

    val customerTbl = "customer"
    val lineitemTbl = "lineitem"
    val nationTbl = "nation"
    val regionTbl = "region"
    val ordersTbl = "orders"
    val ordersPath = "order"
    val partTbl = "part"
    val partsuppTbl = "partsupp"
    val supplierTbl = "supplier"

    println(
      s"""
         |CREATE DATABASE IF NOT EXISTS ${dbName}
         |WITH DBPROPERTIES (engine='Parquet');
         |""".stripMargin)
    println(s"""use ${dbName};""")
    println("")

    // customer
    println(
      s"""
         |DROP TABLE IF EXISTS ${tablePrefix}${customerTbl}${tableSuffix};
         |""".stripMargin)
    println(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}${customerTbl}${tableSuffix} (
         | c_custkey    bigint,
         | c_name       string,
         | c_address    string,
         | c_nationkey  bigint,
         | c_phone      string,
         | c_acctbal    double,
         | c_mktsegment string,
         | c_comment    string)
         | STORED AS PARQUET LOCATION 'file://${dataPathRoot + customerTbl}';
         |""".stripMargin)
    println("")
    println("")

    // lineitem
    println(
      s"""
         |DROP TABLE IF EXISTS ${tablePrefix}${lineitemTbl}${tableSuffix};
         |""".stripMargin)
    println(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}${lineitemTbl}${tableSuffix} (
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
         | STORED AS PARQUET LOCATION 'file://${dataPathRoot + lineitemTbl}';
         |""".stripMargin)
    println("")
    println("")

    // nation
    println(
      s"""
         |DROP TABLE IF EXISTS ${tablePrefix}${nationTbl}${tableSuffix};
         |""".stripMargin)
    println(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}${nationTbl}${tableSuffix} (
         | n_nationkey bigint,
         | n_name      string,
         | n_regionkey bigint,
         | n_comment   string)
         | STORED AS PARQUET LOCATION 'file://${dataPathRoot + nationTbl}';
         |""".stripMargin)
    println("")
    println("")

    // region
    println(
      s"""
         |DROP TABLE IF EXISTS ${tablePrefix}${regionTbl}${tableSuffix};
         |""".stripMargin)
    println(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}${regionTbl}${tableSuffix} (
         | r_regionkey bigint,
         | r_name      string,
         | r_comment   string)
         | STORED AS PARQUET LOCATION 'file://${dataPathRoot + regionTbl}';
         |""".stripMargin)
    println("")
    println("")

    // orders
    println(
      s"""
         |DROP TABLE IF EXISTS ${tablePrefix}${ordersTbl}${tableSuffix};
         |""".stripMargin)
    println(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}${ordersTbl}${tableSuffix} (
         | o_orderkey      bigint,
         | o_custkey       bigint,
         | o_orderstatus   string,
         | o_totalprice    double,
         | o_orderdate     date,
         | o_orderpriority string,
         | o_clerk         string,
         | o_shippriority  bigint,
         | o_comment       string)
         | STORED AS PARQUET LOCATION 'file://${dataPathRoot + ordersPath}';
         |""".stripMargin)
    println("")
    println("")

    // part
    println(
      s"""
         |DROP TABLE IF EXISTS ${tablePrefix}${partTbl}${tableSuffix};
         |""".stripMargin)
    println(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}${partTbl}${tableSuffix} (
         | p_partkey     bigint,
         | p_name        string,
         | p_mfgr        string,
         | p_brand       string,
         | p_type        string,
         | p_size        bigint,
         | p_container   string,
         | p_retailprice double,
         | p_comment     string)
         | STORED AS PARQUET LOCATION 'file://${dataPathRoot + partTbl}';
         |""".stripMargin)
    println("")
    println("")

    // partsupp
    println(
      s"""
         |DROP TABLE IF EXISTS ${tablePrefix}${partsuppTbl}${tableSuffix};
         |""".stripMargin)
    println(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}${partsuppTbl}${tableSuffix} (
         | ps_partkey    bigint,
         | ps_suppkey    bigint,
         | ps_availqty   bigint,
         | ps_supplycost double,
         | ps_comment    string)
         | STORED AS PARQUET LOCATION 'file://${dataPathRoot + partsuppTbl}';
         |""".stripMargin)
    println("")
    println("")

    // supplier
    println(
      s"""
         |DROP TABLE IF EXISTS ${tablePrefix}${supplierTbl}${tableSuffix};
         |""".stripMargin)
    println(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${tablePrefix}${supplierTbl}${tableSuffix} (
         | s_suppkey   bigint,
         | s_name      string,
         | s_address   string,
         | s_nationkey bigint,
         | s_phone     string,
         | s_acctbal   double,
         | s_comment   string)
         | STORED AS PARQUET LOCATION 'file://${dataPathRoot + supplierTbl}';
         |""".stripMargin)
    println("")
    println("")
    // scalastyle:on println
  }

  def genTPCHCSV2ParquetSQL(): Unit = {
    // scalastyle:off println
    val dbName = "gendb"
    val csvPathRoot = "/home/ubuntu/sf1000/"
    val parquetPathRoot = "/home/ubuntu/tpch-data-sf1000/"

    val tablePrefix = "tpchdb."
    val tableSuffix = "1000"

    val customerTbl = "customer"
    val customerParts = "/*+ REPARTITION(24) */"
    val lineitemTbl = "lineitem"
    val lineitemParts = "/*+ REPARTITION(720) */"
    val nationTbl = "nation"
    val nationParts = ""
    val regionTbl = "region"
    val regionParts = ""
    val ordersTbl = "orders"
    val ordersPath = "order"
    val ordersParts = "/*+ REPARTITION(180) */"
    val partTbl = "part"
    val partParts = "/*+ REPARTITION(40) */"
    val partsuppTbl = "partsupp"
    val partsuppParts = "/*+ REPARTITION(120) */"
    val supplierTbl = "supplier"
    val supplierParts = "/*+ REPARTITION(2) */"

    println(s"""CREATE DATABASE IF NOT EXISTS ${dbName};""")
    println(s"""use ${dbName};""")
    println("")

    // customer
    println(s"""DROP TABLE IF EXISTS ${customerTbl}_csv;""")
    println(
      s"""
         |CREATE TABLE IF NOT EXISTS ${customerTbl}_csv (
         | c_custkey    bigint,
         | c_name       string,
         | c_address    string,
         | c_nationkey  bigint,
         | c_phone      string,
         | c_acctbal    double,
         | c_mktsegment string,
         | c_comment    string)
         | USING csv
         | OPTIONS (
         |  path '${csvPathRoot + customerTbl + "/"}',
         |  header false,
         |  sep '|'
         | );
         |""".stripMargin)
    println(s"""DROP TABLE IF EXISTS ${tablePrefix}${customerTbl}${tableSuffix};""")
    println(
      s"""
         |CREATE TABLE IF NOT EXISTS ${tablePrefix}${customerTbl}${tableSuffix}
         | STORED AS PARQUET
         | LOCATION '${parquetPathRoot + customerTbl}'
         | AS SELECT ${customerParts} * FROM ${customerTbl}_csv;
         |""".stripMargin)
    println("")
    println("")

    // lineitem
    println(s"""DROP TABLE IF EXISTS ${lineitemTbl}_csv;""")
    println(
      s"""
         |CREATE TABLE IF NOT EXISTS ${lineitemTbl}_csv (
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
         | USING csv
         | OPTIONS (
         |  path '${csvPathRoot + lineitemTbl + "/"}',
         |  header false,
         |  sep '|'
         | );
         |""".stripMargin)
    println(s"""DROP TABLE IF EXISTS ${tablePrefix}${lineitemTbl}${tableSuffix};""")
    println(
      s"""
         |CREATE TABLE IF NOT EXISTS ${tablePrefix}${lineitemTbl}${tableSuffix}
         | STORED AS PARQUET
         | LOCATION '${parquetPathRoot + lineitemTbl}'
         | AS SELECT ${lineitemParts} * FROM ${lineitemTbl}_csv;
         |""".stripMargin)
    println("")
    println("")

    // nation
    println(s"""DROP TABLE IF EXISTS ${nationTbl}_csv;""")
    println(
      s"""
         |CREATE TABLE IF NOT EXISTS ${nationTbl}_csv (
         | n_nationkey bigint,
         | n_name      string,
         | n_regionkey bigint,
         | n_comment   string)
         | USING csv
         | OPTIONS (
         |  path '${csvPathRoot + nationTbl + "/"}',
         |  header false,
         |  sep '|'
         | );
         |""".stripMargin)
    println(s"""DROP TABLE IF EXISTS ${tablePrefix}${nationTbl}${tableSuffix};""")
    println(
      s"""
         |CREATE TABLE IF NOT EXISTS ${tablePrefix}${nationTbl}${tableSuffix}
         | STORED AS PARQUET
         | LOCATION '${parquetPathRoot + nationTbl}'
         | AS SELECT ${nationParts} * FROM ${nationTbl}_csv;
         |""".stripMargin)
    println("")
    println("")

    // region
    println(s"""DROP TABLE IF EXISTS ${regionTbl}_csv;""")
    println(
      s"""
         |CREATE TABLE IF NOT EXISTS ${regionTbl}_csv (
         | r_regionkey bigint,
         | r_name      string,
         | r_comment   string)
         | USING csv
         | OPTIONS (
         |  path '${csvPathRoot + regionTbl + "/"}',
         |  header false,
         |  sep '|'
         | );
         |""".stripMargin)
    println(s"""DROP TABLE IF EXISTS ${tablePrefix}${regionTbl}${tableSuffix};""")
    println(
      s"""
         |CREATE TABLE IF NOT EXISTS ${tablePrefix}${regionTbl}${tableSuffix}
         | STORED AS PARQUET
         | LOCATION '${parquetPathRoot + regionTbl}'
         | AS SELECT ${regionParts} * FROM ${regionTbl}_csv;
         |""".stripMargin)
    println("")
    println("")

    // orders
    println(s"""DROP TABLE IF EXISTS ${ordersTbl}_csv;""")
    println(
      s"""
         |CREATE TABLE IF NOT EXISTS ${ordersTbl}_csv (
         | o_orderkey      bigint,
         | o_custkey       bigint,
         | o_orderstatus   string,
         | o_totalprice    double,
         | o_orderdate     date,
         | o_orderpriority string,
         | o_clerk         string,
         | o_shippriority  bigint,
         | o_comment       string)
         | USING csv
         | OPTIONS (
         |  path '${csvPathRoot + ordersPath + "/"}',
         |  header false,
         |  sep '|'
         | );
         |""".stripMargin)
    println(s"""DROP TABLE IF EXISTS ${tablePrefix}${ordersTbl}${tableSuffix};""")
    println(
      s"""
         |CREATE TABLE IF NOT EXISTS ${tablePrefix}${ordersTbl}${tableSuffix}
         | STORED AS PARQUET
         | LOCATION '${parquetPathRoot + ordersPath}'
         | AS SELECT ${ordersParts} * FROM ${ordersTbl}_csv;
         |""".stripMargin)
    println("")
    println("")

    // part
    println(s"""DROP TABLE IF EXISTS ${partTbl}_csv;""")
    println(
      s"""
         |CREATE TABLE IF NOT EXISTS ${partTbl}_csv (
         | p_partkey     bigint,
         | p_name        string,
         | p_mfgr        string,
         | p_brand       string,
         | p_type        string,
         | p_size        bigint,
         | p_container   string,
         | p_retailprice double,
         | p_comment     string)
         | USING csv
         | OPTIONS (
         |  path '${csvPathRoot + partTbl + "/"}',
         |  header false,
         |  sep '|'
         | );
         |""".stripMargin)
    println(s"""DROP TABLE IF EXISTS ${tablePrefix}${partTbl}${tableSuffix};""")
    println(
      s"""
         |CREATE TABLE IF NOT EXISTS ${tablePrefix}${partTbl}${tableSuffix}
         | STORED AS PARQUET
         | LOCATION '${parquetPathRoot + partTbl}'
         | AS SELECT ${partParts} * FROM ${partTbl}_csv;
         |""".stripMargin)
    println("")
    println("")

    // partsupp
    println(s"""DROP TABLE IF EXISTS ${partsuppTbl}_csv;""")
    println(
      s"""
         |CREATE TABLE IF NOT EXISTS ${partsuppTbl}_csv (
         | ps_partkey    bigint,
         | ps_suppkey    bigint,
         | ps_availqty   bigint,
         | ps_supplycost double,
         | ps_comment    string)
         | USING csv
         | OPTIONS (
         |  path '${csvPathRoot + partsuppTbl + "/"}',
         |  header false,
         |  sep '|'
         | );
         |""".stripMargin)
    println(s"""DROP TABLE IF EXISTS ${tablePrefix}${partsuppTbl}${tableSuffix};""")
    println(
      s"""
         |CREATE TABLE IF NOT EXISTS ${tablePrefix}${partsuppTbl}${tableSuffix}
         | STORED AS PARQUET
         | LOCATION '${parquetPathRoot + partsuppTbl}'
         | AS SELECT ${partsuppParts} * FROM ${partsuppTbl}_csv;
         |""".stripMargin)
    println("")
    println("")

    // supplier
    println(s"""DROP TABLE IF EXISTS ${supplierTbl}_csv;""")
    println(
      s"""
         |CREATE TABLE IF NOT EXISTS ${supplierTbl}_csv (
         | s_suppkey   bigint,
         | s_name      string,
         | s_address   string,
         | s_nationkey bigint,
         | s_phone     string,
         | s_acctbal   double,
         | s_comment   string)
         | USING csv
         | OPTIONS (
         |  path '${csvPathRoot + supplierTbl + "/"}',
         |  header false,
         |  sep '|'
         | );
         |""".stripMargin)
    println(s"""DROP TABLE IF EXISTS ${tablePrefix}${supplierTbl}${tableSuffix};""")
    println(
      s"""
         |CREATE TABLE IF NOT EXISTS ${tablePrefix}${supplierTbl}${tableSuffix}
         | STORED AS PARQUET
         | LOCATION '${parquetPathRoot + supplierTbl}'
         | AS SELECT ${supplierParts} * FROM ${supplierTbl}_csv;
         |""".stripMargin)
    println("")
    println("")
    // scalastyle:on println
  }

}
