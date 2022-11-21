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
    ).map { table =>
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
      .set("spark.unsafe.exceptionOnMemoryLeak", "false")
      .set("spark.sql.autoBroadcastJoinThreshold", "10M")
      .set("spark.sql.sources.useV1SourceList", "avro")
  }

  test("Bool type") {
    // Validation: BatchScan Filter Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, bool from type1 where bool == true  " +
      " group by grouping sets(int, bool) sort by int, bool limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.bool from type1," +
      " type2 where type1.bool = type2.bool") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.bool from type1," +
      " type2 where type1.bool = type2.bool") { _ => }
  }

  test("Binary type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, binary from type1 " +
      " group by grouping sets(int, binary) sort by int, binary limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.binary from type1," +
      " type2 where type1.binary = type2.binary") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.binary from type1," +
      " type2 where type1.binary = type2.binary") { _ => }
  }

  test("String type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, string from type1" +
      " group by grouping sets(int, string) sort by int, string limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.string from type1," +
      " type2 where type1.string = type2.string") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.string from type1," +
      " type2 where type1.string = type2.string") { _ => }
  }


  test("Double type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, double from type1 " +
      " group by grouping sets(int, double) sort by int, double limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.double from type1," +
      " type2 where type1.double = type2.double") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.double from type1," +
      " type2 where type1.double = type2.double") { _ => }
  }

  test("Float type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, float from type1 " +
      " group by grouping sets(int, float) sort by int, float limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.float from type1," +
      " type2 where type1.float = type2.float") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.float from type1," +
      " type2 where type1.float = type2.float") { _ => }
  }

  test("Long type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, long from type1 " +
      " group by grouping sets(int, long) sort by int, long limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.long from type1," +
      " type2 where type1.long = type2.long") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.long from type1," +
      " type2 where type1.long = type2.long") { _ => }
  }

  test("Int type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, short from type1 " +
      " group by grouping sets(int, short) sort by short, int limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.int from type1," +
      " type2 where type1.int = type2.int") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.int from type1," +
      " type2 where type1.int = type2.int") { _ => }
  }

  test("Short type") {
    // Validation: BatchScan Project Aggregate Expand Sort Limit
    runQueryAndCompare("select int, short from type1 " +
      " group by grouping sets(int, short) sort by short, int limit 1") { _ => }

    // Validation: BroadHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "10M")
    runQueryAndCompare("select type1.short from type1," +
      " type2 where type1.short = type2.short") { _ => }

    // Validation: ShuffledHashJoin, Filter, Project
    super.sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    runQueryAndCompare("select type1.short from type1," +
      " type2 where type1.short = type2.short") { _ => }
  }
}
