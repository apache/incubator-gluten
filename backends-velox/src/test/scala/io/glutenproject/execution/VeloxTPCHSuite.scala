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

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.ColumnarInputAdapter
import org.apache.spark.sql.types.DoubleType

import scala.io.Source

abstract class VeloxTPCHSuite extends WholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  // TODO: the tpch query was changed a bit. Because date was converted into string in the test
  //  dataset, the queries were changed accordingly.
  protected val veloxTPCHQueries: String = rootPath + "/tpch-queries-velox"

  // TODO: result comparison is not supported currently.
  protected val queriesResults: String = rootPath + "queries-output"

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
      .set("spark.unsafe.exceptionOnMemoryLeak", "false")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  test("TPC-H q1") {
    runTPCHQuery(1, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q1 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(1, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q2") {
    runTPCHQuery(2, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q2 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(2, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q3") {
    runTPCHQuery(3, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q3 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(3, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q4") {
    runTPCHQuery(4, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q4 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(4, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q5") {
    runTPCHQuery(5, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q5 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(5, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q6") {
    runTPCHQuery(6, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q6 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(6, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q7") {
    runTPCHQuery(7, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q7 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(7, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q8") {
    runTPCHQuery(8, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q8 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(8, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q9") {
    runTPCHQuery(9, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q9 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(9, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q10") {
    runTPCHQuery(10, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q10 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(10, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q11") {
    runTPCHQuery(11, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q11 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(11, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q12") {
    runTPCHQuery(12, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q12 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(12, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q13") {
    runTPCHQuery(13, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q13 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(13, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q14") {
    runTPCHQuery(14, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q14 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(14, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q15") {
    runTPCHQuery(15, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q15 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(15, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q16") {
    runTPCHQuery(16, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q16 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(16, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q17") {
    runTPCHQuery(17, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q17 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(17, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q18") {
    runTPCHQuery(18, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q18 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(18, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q19") {
    runTPCHQuery(19, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q19 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(19, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q20") {
    runTPCHQuery(20, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q20 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(20, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q21") {
    runTPCHQuery(21, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  // TODO: fix q21 with partitions size == 1 when bhj enabled.
  ignore("TPC-H q21 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(21, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }

  test("TPC-H q22") {
    runTPCHQuery(22, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
  }

  test("TPC-H q22 - bhj enable") {
    withSQLConf(("spark.sql.autoBroadcastJoinThreshold", "30M")) {
      runTPCHQuery(22, veloxTPCHQueries, queriesResults, compareResult = false) { _ => }
    }
  }
}

class VeloxTPCHV1Suite extends VeloxTPCHSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "orc")
  }
}

class VeloxTPCHV2Suite extends VeloxTPCHSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "avro")
  }
}
