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
import org.apache.spark.sql.{Row, TestUtils}

abstract class VeloxTPCHTableSupport extends VeloxWholeStageTransformerSuite {
  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  // TODO: the tpch query was changed a bit. Because date was converted into string in the test
  //  dataset, the queries were changed accordingly.
  protected val veloxTPCHQueries: String = rootPath + "/tpch-queries-velox"

  // TODO: result comparison is not supported currently.
  protected val queriesResults: String = rootPath + "queries-output"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()
  }
}

abstract class VeloxTPCHSuite extends VeloxTPCHTableSupport {
  test("TPC-H q1") {
    runTPCHQuery(1, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q2") {
    runTPCHQuery(2, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q3") {
    runTPCHQuery(3, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q4") {
    runTPCHQuery(4, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q5") {
    runTPCHQuery(5, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q6") {
    runTPCHQuery(6, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q7") {
    runTPCHQuery(7, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q8") {
    runTPCHQuery(8, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q9") {
    runTPCHQuery(9, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q10") {
    runTPCHQuery(10, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q11") {
    runTPCHQuery(11, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q12") {
    runTPCHQuery(12, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q13") {
    runTPCHQuery(13, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q14") {
    runTPCHQuery(14, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q15") {
    runTPCHQuery(15, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q16") {
    runTPCHQuery(16, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q17") {
    runTPCHQuery(17, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q18") {
    runTPCHQuery(18, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q19") {
    runTPCHQuery(19, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q20") {
    runTPCHQuery(20, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q21") {
    runTPCHQuery(21, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("TPC-H q22") {
    runTPCHQuery(22, veloxTPCHQueries, queriesResults, compareResult = false, noFallBack = false) {
      _ =>
    }
  }

  test("test 'order by limit'") {
    val df = spark.sql(
      """
        |select n_nationkey from nation order by n_nationkey limit 5
        |""".stripMargin
    )
    val sortExec = df.queryExecution.executedPlan.collect {
      case sortExec: TakeOrderedAndProjectExecTransformer => sortExec
    }
    assert(sortExec.size == 1)
    val result = df.collect()
    val expectedResult = Seq(Row(0), Row(1), Row(2), Row(3), Row(4))
    TestUtils.compareAnswers(result, expectedResult)
  }
}

class VeloxTPCHDistinctSpill extends VeloxTPCHTableSupport {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.memory.offHeap.size", "50m")
      .set("spark.gluten.memory.overAcquiredMemoryRatio", "0.9") // to trigger distinct spill early
  }

  test("distinct spill") {
    val df = spark.sql("select count(distinct *) from lineitem limit 1")
    TestUtils.compareAnswers(df.collect(), Seq(Row(60175)))
  }
}

class VeloxTPCHV1Suite extends VeloxTPCHSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
  }
}

class VeloxTPCHV1BhjSuite extends VeloxTPCHSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "")
      .set("spark.sql.autoBroadcastJoinThreshold", "30M")
  }
}

class VeloxTPCHV2Suite extends VeloxTPCHSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
  }
}

class VeloxTPCHV2BhjSuite extends VeloxTPCHSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "")
      .set("spark.sql.autoBroadcastJoinThreshold", "30M")
  }
}
