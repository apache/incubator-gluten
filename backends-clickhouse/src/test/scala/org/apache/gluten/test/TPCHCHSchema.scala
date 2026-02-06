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
package org.apache.gluten.test

import java.util.Locale

class DDLBuilder private (
    val schema: TPCHCHSchema,
    var tableName: String,
    var format: String,
    var location: String,
    var isNull: Boolean = true,
    var props: Map[String, String] = Map.empty[String, String],
    var tableKey: Option[String] = None,
    var clusterKey: Option[String] = None,
    var numberBuckets: Option[Int] = None,
    var sortByOfBuckets: Seq[String] = Seq.empty
) {
  def this(schema: TPCHCHSchema, tableName: String, format: String, location: String) =
    this(schema, tableName, format, location, true)

  def withIsNull(isNull: Boolean): DDLBuilder = {
    this.isNull = isNull
    this
  }

  def withProps(props: Map[String, String]): DDLBuilder = {
    this.props = props
    this
  }

  def withTableKey(tableKey: String): DDLBuilder = {
    assert(tableKey.nonEmpty)
    this.tableKey = Some(tableKey)
    this
  }

  def withClusterKey(clusters: (String, Int)): DDLBuilder = {
    assert(clusters._1.nonEmpty && clusters._2 > 0)
    this.clusterKey = Some(clusters._1)
    this.numberBuckets = Some(clusters._2)
    this
  }

  def withSortByOfBuckets(sortByColsOfBuckets: Seq[String]): DDLBuilder = {
    if (sortByColsOfBuckets.nonEmpty) {
      this.sortByOfBuckets = sortByColsOfBuckets
    }
    this
  }

  def build(): String = {
    val UnknownTableMsg = s"Unknown table: $tableName"
    val key = tableKey.getOrElse(tableName)

    val tableDefinition = schema.table2columns.get(key) match {
      case Some(columns) =>
        s"""CREATE TABLE IF NOT EXISTS $tableName (
           | ${columns(isNull)} )
           | USING ${format.toUpperCase(Locale.ROOT)}
           | LOCATION '$location'""".stripMargin
      case None => throw new IllegalArgumentException(UnknownTableMsg)
    }

    val tblProperties: String =
      if (props.isEmpty) ""
      else {
        val p = props.map { case (k, v) => s"$k=$v" }.mkString("TBLPROPERTIES (", ", ", ")")
        s"\n$p"
      }

    val cluster = clusterKey
      .flatMap {
        cluster =>
          numberBuckets.map {
            buckets =>
              val clusterKeyStr = s"\nCLUSTERED BY ($cluster)"
              val numberBucketsStr = s" INTO $buckets BUCKETS"
              if (sortByOfBuckets.isEmpty) {
                s"$clusterKeyStr$numberBucketsStr"
              } else {
                val sortByOfBucketsStr = s" SORTED BY (${sortByOfBuckets.mkString(", ")})"
                s"$clusterKeyStr$sortByOfBucketsStr$numberBucketsStr"
              }
          }
      }
      .getOrElse("")

    s"$tableDefinition$cluster$tblProperties"
  }
}

trait TPCHCHSchema {

  val tpchTables: Seq[String] = Seq(
    "customer",
    "lineitem",
    "nation",
    "region",
    "orders",
    "part",
    "partsupp",
    "supplier"
  )

  val table2columns: Map[String, Boolean => String] = Map(
    "customer" -> customer,
    "lineitem" -> lineitem,
    "nation" -> nation,
    "region" -> region,
    "orders" -> orders,
    "part" -> part,
    "partsupp" -> partsupp,
    "supplier" -> supplier
  )

  def createTableBuilder(
      tableName: String,
      format: String,
      location: String
  ): DDLBuilder = {
    new DDLBuilder(this, tableName, format, location)
  }

  private def nullable_(isNull: Boolean): String = if (isNull) {
    ""
  } else {
    "not null"
  }

  def lineitem(isNull: Boolean): String = {
    val nullable = nullable_(isNull)
    s"""
       | l_orderkey      bigint $nullable,
       | l_partkey       bigint $nullable,
       | l_suppkey       bigint $nullable,
       | l_linenumber    bigint $nullable,
       | l_quantity      double $nullable,
       | l_extendedprice double $nullable,
       | l_discount      double $nullable,
       | l_tax           double $nullable,
       | l_returnflag    string $nullable,
       | l_linestatus    string $nullable,
       | l_shipdate      date   $nullable,
       | l_commitdate    date   $nullable,
       | l_receiptdate   date   $nullable,
       | l_shipinstruct  string $nullable,
       | l_shipmode      string $nullable,
       | l_comment       string $nullable
       |""".stripMargin
  }

  def part(isNull: Boolean): String = {
    val nullable = nullable_(isNull)
    s"""
       | p_partkey       bigint $nullable,
       | p_name          string $nullable,
       | p_mfgr          string $nullable,
       | p_brand         string $nullable,
       | p_type          string $nullable,
       | p_size          bigint $nullable,
       | p_container     string $nullable,
       | p_retailprice   double $nullable,
       | p_comment       string $nullable
       |""".stripMargin
  }

  def supplier(isNull: Boolean): String = {
    val nullable = nullable_(isNull)
    s"""
       | s_suppkey      bigint $nullable,
       | s_name         string $nullable,
       | s_address      string $nullable,
       | s_nationkey    bigint $nullable,
       | s_phone        string $nullable,
       | s_acctbal      double $nullable,
       | s_comment      string $nullable
       |""".stripMargin
  }

  def partsupp(isNull: Boolean): String = {
    val nullable = nullable_(isNull)
    s"""
       | ps_partkey     bigint $nullable,
       | ps_suppkey     bigint $nullable,
       | ps_availqty    bigint $nullable,
       | ps_supplycost  double $nullable,
       | ps_comment     string $nullable
       |""".stripMargin
  }

  def customer(isNull: Boolean): String = {
    val nullable = nullable_(isNull)
    s"""
       | c_custkey      bigint $nullable,
       | c_name         string $nullable,
       | c_address      string $nullable,
       | c_nationkey    bigint $nullable,
       | c_phone        string $nullable,
       | c_acctbal      double $nullable,
       | c_mktsegment   string $nullable,
       | c_comment      string $nullable
       |""".stripMargin
  }

  def orders(isNull: Boolean): String = {
    val nullable = nullable_(isNull)
    s"""
       | o_orderkey      bigint $nullable,
       | o_custkey       bigint $nullable,
       | o_orderstatus   string $nullable,
       | o_totalprice    double $nullable,
       | o_orderdate     date   $nullable,
       | o_orderpriority string $nullable,
       | o_clerk         string $nullable,
       | o_shippriority  bigint $nullable,
       | o_comment       string $nullable
       |""".stripMargin
  }

  def nation(isNull: Boolean): String = {
    val nullable = nullable_(isNull)
    s"""
       | n_nationkey     bigint $nullable,
       | n_name          string $nullable,
       | n_regionkey     bigint $nullable,
       | n_comment       string $nullable
       |""".stripMargin
  }

  def region(isNull: Boolean): String = {
    val nullable = nullable_(isNull)
    s"""
       | r_regionkey     bigint $nullable,
       | r_name          string $nullable,
       | r_comment       string $nullable
       |""".stripMargin
  }
}
