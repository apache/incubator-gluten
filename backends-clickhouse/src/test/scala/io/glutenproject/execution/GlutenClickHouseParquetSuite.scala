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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructType}

import java.util.Date

import scala.language.implicitConversions

class GlutenClickHouseParquetSuite extends GlutenClickHouseTPCHAbstractSuite {
  import testImplicits._

  override protected val resourcePath: String =
    "../../../../gluten-core/src/test/resources/tpch-data"

  override protected val tablesPath: String = basePath + "/tpch-data"
  override protected val tpchQueries: String =
    rootPath + "../../../../gluten-core/src/test/resources/tpch-queries"
  override protected val queriesResults: String = rootPath + "queries-output"

  override protected def createTPCHNullableTables(): Unit = {}

  override protected def createTPCHNotNullTables(): Unit = {}

  test("empty parquet") {
    val df = spark.read.parquet(createEmptyParquet()).toDF().select($"a")
    assert(df.collect().isEmpty)
  }

  def createEmptyParquet(): String = {
    val data = spark.sparkContext.emptyRDD[Row]
    val schema = new StructType()
      .add("a", StringType)

    val fileName = basePath + "/parquet_test_" + new Date().getTime + "_empty.parquet"

    spark.createDataFrame(data, schema).toDF().write.parquet(fileName)
    fileName
  }

}
