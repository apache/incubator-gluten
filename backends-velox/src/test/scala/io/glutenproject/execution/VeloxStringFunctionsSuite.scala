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
import org.apache.spark.sql.DataFrame

class VeloxStringFunctionsSuite extends WholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

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
      .set("spark.sql.sources.useV1SourceList", "avro")
  }

  def checkLengthAndPlan(df: DataFrame) {
    this.checkLengthAndPlan(df, 5)
  }

  test("ascii") {
    runQueryAndCompare("select l_orderkey, ascii(l_comment) " +
      "from lineitem limit 5") { checkLengthAndPlan }

    runQueryAndCompare("select l_orderkey, ascii(null) " +
      "from lineitem limit 5") { checkLengthAndPlan }
  }

  test("concat") {
    runQueryAndCompare("select l_orderkey, concat(l_comment, 'hello') " +
      "from lineitem limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, concat(l_comment, 'hello', 'world') " +
      "from lineitem limit 5") { checkLengthAndPlan }
  }

  test("instr") {
    runQueryAndCompare("select l_orderkey, instr(l_comment, 'h') " +
      "from lineitem limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, instr(l_comment, null) " +
      "from lineitem limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, instr(null, 'h') " +
      "from lineitem limit 5") { checkLengthAndPlan }
  }

  test("length") {
    runQueryAndCompare("select l_orderkey, length(l_comment) " +
      "from lineitem limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, length(null) " +
      "from lineitem limit 5") { checkLengthAndPlan }
  }

  test("lower") {
    runQueryAndCompare("select l_orderkey, lower(l_comment) " +
      "from lineitem limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, lower(null) " +
      "from lineitem limit 5") { checkLengthAndPlan }
  }

  test("upper") {
    runQueryAndCompare("select l_orderkey, upper(l_comment) " +
      "from lineitem limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, upper(null) " +
      "from lineitem limit 5") { checkLengthAndPlan }
  }

  test("like") {
    runQueryAndCompare("select l_orderkey, like(l_comment, '%a%') " +
      "from lineitem limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, like(l_comment, ' ') " +
      "from lineitem limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, like(null, '%a%') " +
      "from lineitem limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, l_comment " +
      "from lineitem where l_comment like '%a%' limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, like(l_comment, ' ') " +
      "from lineitem where l_comment like ''  limit 5") { _ => }
    runQueryAndCompare("select l_orderkey, like(null, '%a%') " +
      "from lineitem where l_comment like '%$$$##@@#&&' limit 5") { _ => }
  }

  test("rlike") {
    runQueryAndCompare("select l_orderkey, l_comment, rlike(l_comment, 'a*') " +
      "from lineitem limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, rlike(l_comment, ' ') " +
      "from lineitem limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, rlike(null, '%a%') " +
      "from lineitem limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, l_comment " +
      "from lineitem where l_comment rlike '%a%' limit 5") { _ => }
    runQueryAndCompare("select l_orderkey, like(l_comment, ' ') " +
      "from lineitem where l_comment rlike ''  limit 5") { _ => }
    runQueryAndCompare("select l_orderkey, like(null, '%a%') " +
      "from lineitem where l_comment rlike '%$$$##@@#&&' limit 5") { _ => }
  }

  test("regexp_extract") {
    runQueryAndCompare("select l_orderkey, regexp_extract(l_comment, '([a-z])', 1) " +
      "from lineitem limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, regexp_extract(null, '([a-z])', 1) " +
      "from lineitem limit 5") { checkLengthAndPlan }
  }

  test("replace") {
    runQueryAndCompare("select l_orderkey, replace(l_comment, ' ', 'hello') " +
      "from lineitem limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, replace(l_comment, 'ha') " +
      "from lineitem limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, replace(l_comment, ' ', null) " +
      "from lineitem limit 5") { checkLengthAndPlan }
    runQueryAndCompare("select l_orderkey, replace(l_comment, null, 'hello') " +
      "from lineitem limit 5") { checkLengthAndPlan }
  }

  test("split") {
    val df = runQueryAndCompare("select l_orderkey, split(l_comment, 'h', 3) " +
      "from lineitem limit 5") { _ => }
    assert(df.collect().length == 5)
  }

}
