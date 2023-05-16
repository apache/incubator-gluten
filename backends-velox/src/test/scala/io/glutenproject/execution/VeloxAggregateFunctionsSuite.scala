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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{avg, col}
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}

import scala.collection.JavaConverters

class VeloxAggregateFunctionsSuite extends WholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.gluten.sql.validate.failure.logLevel", "WARN")
  }

  private lazy val person3: DataFrame = Seq(
    ("Luis", 1, 99),
    ("Luis", 16, 99),
    ("Luis", 16, 176),
    ("Fernando", 32, 99),
    ("Fernando", 32, 164),
    ("David", 60, 99),
    ("Amy", 24, 99)).toDF("name", "age", "height")

  test("SPARK-34165: Add count_distinct to summary") {
    val summaryDF = person3.summary("count")

    val summaryResult = Seq(
      Row("count", "7", "7", "7"))

    def getSchemaAsSeq(df: DataFrame): Seq[String] = df.schema.map(_.name)

    assert(getSchemaAsSeq(summaryDF) === Seq("summary", "name", "age", "height"))
    checkAnswer(summaryDF, summaryResult)
  }
}
