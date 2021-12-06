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
package com.intel.oap.spark.sql.execution.datasources.arrow

import java.util.concurrent.{Executors, TimeUnit}

import com.intel.oap.spark.sql.DataFrameReaderImplicits._
import com.intel.oap.spark.sql.execution.datasources.v2.arrow.{ArrowOptions, ArrowUtils}

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ArrowDataSourceTPCHBasedTest extends QueryTest with SharedSparkSession {

  // tpc-h query cases: generated tpc-h dataset required
  private val prefix = "/root/Downloads/"
  private val tpchFolder = "date_tpch_10"
  private val lineitem = prefix + tpchFolder + "/lineitem"
  private val part = prefix + tpchFolder + "/part"
  private val partSupp = prefix + tpchFolder + "/partsupp"
  private val supplier = prefix + tpchFolder + "/supplier"
  private val orders = prefix + tpchFolder + "/orders"
  private val nation = prefix + tpchFolder + "/nation"


  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
    conf.set("spark.memory.offHeap.size", String.valueOf(128 * 1024 * 1024))
    conf
  }

  ignore("tpch lineitem - desc") {
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(lineitem)
    frame.createOrReplaceTempView("lineitem")

    spark.sql("describe lineitem").show()
  }

  ignore("tpch part - special characters in path") {
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(part)
    frame.createOrReplaceTempView("part")

    spark.sql("select * from part limit 100").show()
  }

  ignore("tpch lineitem - read partition values") {
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(orders)
    frame.createOrReplaceTempView("orders")

    spark.sql("select o_orderdate from orders limit 100").show()
  }

  ignore("tpch lineitem - asterisk select") {
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(lineitem)
    frame.createOrReplaceTempView("lineitem")

    spark.sql("select * from lineitem limit 10").show()
  }

  ignore("tpch query 6") {
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(lineitem)
    frame.createOrReplaceTempView("lineitem")

    spark.sql("select\n\tsum(l_extendedprice * l_discount) as revenue\n" +
      "from\n\tlineitem\n" +
      "where\n\tl_shipdate >= date '1994-01-01'\n\t" +
      "and l_shipdate < date '1994-01-01' + interval '1' year\n\t" +
      "and l_discount between .06 - 0.01 and .06 + 0.01\n\t" +
      "and l_quantity < 24").show()
  }

  ignore("tpch query 6 - performance comparision") {
    val iterations = 10
    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "false") {
      val frame1 = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
        .arrow(lineitem)
      frame1.createOrReplaceTempView("lineitem_arrow")

      val frame2 = spark.read
        .parquet(lineitem)
      frame2.createOrReplaceTempView("lineitem_parquet")

      val pPrev = System.currentTimeMillis()
      (0 until iterations).foreach(_ =>
        spark.sql("select\n\tsum(l_extendedprice * l_discount) as revenue\n" +
          "from\n\tlineitem_parquet\n" +
          "where\n\tl_shipdate >= date '1994-01-01'\n\t" +
          "and l_shipdate < date '1994-01-01' + interval '1' year\n\t" +
          "and l_discount between .06 - 0.01 and .06 + 0.01\n\t" +
          "and l_quantity < 24").show()
      )
      val parquetExecTime = System.currentTimeMillis() - pPrev

      val aPrev = System.currentTimeMillis()
      (0 until iterations).foreach(_ => {
        // scalastyle:off println
        println(SparkMemoryUtils.contextAllocator().getAllocatedMemory)
        // scalastyle:on println
        spark.sql("select\n\tsum(l_extendedprice * l_discount) as revenue\n" +
          "from\n\tlineitem_arrow\n" +
          "where\n\tl_shipdate >= date '1994-01-01'\n\t" +
          "and l_shipdate < date '1994-01-01' + interval '1' year\n\t" +
          "and l_discount between .06 - 0.01 and .06 + 0.01\n\t" +
          "and l_quantity < 24").show()
      }
      )
      val arrowExecTime = System.currentTimeMillis() - aPrev

      // unstable assert
      assert(arrowExecTime < parquetExecTime)
    }
  }

  ignore("tpch query 16 - performance comparision") {
    val iterations = 1
    withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> "false") {
      val frame1 = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
        .arrow(partSupp)
      frame1.createOrReplaceTempView("partsupp_arrow")

      val frame2 = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
        .arrow(part)
      frame2.createOrReplaceTempView("part_arrow")

      val frame3 = spark.read
        .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
        .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
        .arrow(supplier)
      frame3.createOrReplaceTempView("supplier_arrow")

      val frame4 = spark.read
        .parquet(partSupp)
      frame4.createOrReplaceTempView("partsupp_parquet")

      val frame5 = spark.read
        .parquet(part)
      frame5.createOrReplaceTempView("part_parquet")

      val frame6 = spark.read
        .parquet(supplier)
      frame6.createOrReplaceTempView("supplier_parquet")

      val pPrev = System.currentTimeMillis()
      (0 until iterations).foreach(_ =>
        spark.sql("select\n\tp_brand,\n\tp_type,\n\tp_size," +
          "\n\tcount(distinct ps_suppkey) as supplier_cnt\n" +
          "from\n\tpartsupp_parquet,\n\tpart_parquet\nwhere\n\tp_partkey" +
          " = ps_partkey\n\tand p_brand <> 'Brand#45'\n\t" +
          "and p_type not like 'MEDIUM POLISHED%'\n\tand p_size in " +
          "(49, 14, 23, 45, 19, 3, 36, 9)\n\t" +
          "and ps_suppkey not in (\n\t\tselect\n\t\t\ts_suppkey\n\t\t" +
          "from\n\t\t\tsupplier_parquet\n\t\twhere\n\t\t\t" +
          "s_comment like '%Customer%Complaints%'\n\t)\ngroup by\n\t" +
          "p_brand,\n\tp_type,\n\tp_size\norder by\n\t" +
          "supplier_cnt desc,\n\tp_brand,\n\tp_type,\n\tp_size").show()
      )
      val parquetExecTime = System.currentTimeMillis() - pPrev

      val aPrev = System.currentTimeMillis()
      (0 until iterations).foreach(_ =>
        spark.sql("select\n\tp_brand,\n\tp_type,\n\tp_size," +
          "\n\tcount(distinct ps_suppkey) as supplier_cnt\n" +
          "from\n\tpartsupp_arrow,\n\tpart_arrow\nwhere\n\tp_partkey" +
          " = ps_partkey\n\tand p_brand <> 'Brand#45'\n\t" +
          "and p_type not like 'MEDIUM POLISHED%'\n\tand p_size in " +
          "(49, 14, 23, 45, 19, 3, 36, 9)\n\t" +
          "and ps_suppkey not in (\n\t\tselect\n\t\t\ts_suppkey\n\t\t" +
          "from\n\t\t\tsupplier_arrow\n\t\twhere\n\t\t\t" +
          "s_comment like '%Customer%Complaints%'\n\t)\ngroup by\n\t" +
          "p_brand,\n\tp_type,\n\tp_size\norder by\n\t" +
          "supplier_cnt desc,\n\tp_brand,\n\tp_type,\n\tp_size").show()
      )
      val arrowExecTime = System.currentTimeMillis() - aPrev

      // scalastyle:off println
      println(arrowExecTime)
      println(parquetExecTime)
      // scalastyle:on println
      // unstable assert
      assert(arrowExecTime < parquetExecTime)
    }
  }

  ignore("tpch query 1") {
    val frame = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(lineitem)
    frame.createOrReplaceTempView("lineitem")

    spark.sql("select\n\tl_returnflag,\n\tl_linestatus," +
      "\n\tsum(l_quantity) as sum_qty,\n\t" +
      "sum(l_extendedprice) as sum_base_price," +
      "\n\tsum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n\t" +
      "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge," +
      "\n\tavg(l_quantity) as avg_qty,\n\t" +
      "avg(l_extendedprice) as avg_price,\n\tavg(l_discount) as avg_disc," +
      "\n\tcount(*) as count_order\nfrom\n\t" +
      "lineitem\nwhere\n\tl_shipdate <= date '1998-12-01' - interval '90' day" +
      "\ngroup by\n\tl_returnflag,\n\t" +
      "l_linestatus\norder by\n\tl_returnflag,\n\tl_linestatus").explain(true)
  }

  ignore("tpch query 21 - memory leak") {
    val frame1 = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(supplier)
    frame1.createOrReplaceTempView("supplier")
    val frame2 = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(lineitem)
    frame2.createOrReplaceTempView("lineitem")
    val frame3 = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(orders)
    frame3.createOrReplaceTempView("orders")
    val frame4 = spark.read
      .option(ArrowOptions.KEY_ORIGINAL_FORMAT, "parquet")
      .option(ArrowOptions.KEY_FILESYSTEM, "hdfs")
      .arrow(nation)
    frame4.createOrReplaceTempView("nation")

    Executors.newSingleThreadExecutor().execute(() => {
      spark.sql("select\n\ts_name,\n\tcount(*) as numwait\nfrom\n\tsupplier,\n\t" +
        "lineitem l1,\n\torders,\n\tnation\nwhere\n\ts_suppkey = l1.l_suppkey\n\t" +
        "and o_orderkey = l1.l_orderkey\n\tand o_orderstatus = 'F'\n\tand " +
        "l1.l_receiptdate > l1.l_commitdate\n\tand exists (\n\t\tselect\n\t\t\t*\n\t\tfrom\n\t\t\t" +
        "lineitem l2\n\t\twhere\n\t\t\tl2.l_orderkey = l1.l_orderkey\n\t\t\tand " +
        "l2.l_suppkey <> l1.l_suppkey\n\t)\n\tand not exists (\n\t\tselect\n\t\t\t*\n\t\t" +
        "from\n\t\t\tlineitem l3\n\t\twhere\n\t\t\tl3.l_orderkey = l1.l_orderkey\n\t\t\t" +
        "and l3.l_suppkey <> l1.l_suppkey\n\t\t\tand l3.l_receiptdate > " +
        "l3.l_commitdate\n\t)\n\tand s_nationkey = n_nationkey\n\tand n_name = 'SAUDI ARABIA'\n" +
        "group by\n\ts_name\norder by\n\tnumwait desc,\n\t" +
        "s_name\nlimit 100").show()
    })
    Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(() => {
      println("[org.apache.spark.sql.util.ArrowUtils.rootAllocator]                           " +
        "Allocated memory amount: " + SparkMemoryUtils.contextAllocator())
      println("[com.intel.oap.vectorized.ArrowWritableColumnVector.allocator] " +
        "Allocated memory amount: " + SparkMemoryUtils.contextAllocator().getAllocatedMemory)
    }, 0L, 100L, TimeUnit.MILLISECONDS)
    Thread.sleep(60 * 60 * 1000L)
  }

}
