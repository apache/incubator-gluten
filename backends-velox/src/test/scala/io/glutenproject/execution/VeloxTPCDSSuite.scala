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

import java.io.File

import scala.io.Source

import org.junit.Ignore

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf

// just used to test TPCDS locally
class VeloxTPCDSSuite extends WholeStageTransformerSuite {

  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tmp/tpcds-generated/"
  override protected val fileFormat: String = "parquet"

  protected var queryTables: Map[String, DataFrame] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    createQueryTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "4g")
      .set("spark.sql.shuffle.partitions", "256")
      .set("spark.memory.offHeap.size", "200g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "false")
      .set("spark.sql.autoBroadcastJoinThreshold", "10M")
      .set("spark.driver.maxResultSize", "4g")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("park.sql.adaptive.enabled", "true")
      .set("spark.gluten.sql.columnar.shuffleSplitDefaultSize", "8192")
      .set("spark.executor.memory", "4g")
      .set("spark.executor.instances", "16")
      .set("spark.executor.cores", "8")
      .set("spark.driver.memory", "20g")
      .set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
  }

  protected def createQueryTables(): Unit = {
    queryTables = Seq(
      "call_center",
      "catalog_page",
      "catalog_returns",
      "catalog_sales",
      "customer",
      "customer_address",
      "customer_demographics",
      "date_dim",
      "household_demographics",
      "income_band",
      "inventory",
      "item",
      "promotion",
      "reason",
      "ship_mode",
      "store",
      "store_returns",
      "store_sales",
      "time_dim",
      "warehouse",
      "web_page",
      "web_returns",
      "web_sales",
      "web_site"
    ).map { table =>
      val tablePath = new File(resourcePath, table).getAbsolutePath
      val tableDF = spark.read.format(fileFormat).load(tablePath)
      tableDF.createOrReplaceTempView(table)
      (table, tableDF)
    }.toMap
  }

  ignore("q7") {
    val queryPath = getClass.getResource("/").getPath +
      "../../../workload/tpcds/tpcds.queries.updated/"
    val source = Source.fromFile(queryPath + "q7.sql")
    val sql = source.mkString
    source.close()
    assert(spark.sql(sql).collect().length == 100)
  }

}
