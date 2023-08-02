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

import java.io.File

class VeloxDataTypeValidationSuite extends WholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/data-type-validation-data"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createDataTypeTable()
  }

  protected def createDataTypeTable(): Unit = {
    TPCHTables = Seq(
      "type1",
      "type2"
    ).map {
      table =>
        val tableDir = getClass.getResource(resourcePath).getFile
        val tablePath = new File(tableDir, table).getAbsolutePath
        val tableDF = spark.read.format(fileFormat).load(tablePath)
        tableDF.createOrReplaceTempView(table)
        (table, tableDF)
    }.toMap
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "10M")
      .set("spark.sql.sources.useV1SourceList", "avro")
  }

  test("Date type") {

    // Validation: ShuffledHashJoin.
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      runQueryAndCompare(
        "select type1.date from type1," +
          " type2 where type1.date = type2.date") {
        checkOperatorMatch[ShuffledHashJoinExecTransformer]
      }
    }

  }

}
