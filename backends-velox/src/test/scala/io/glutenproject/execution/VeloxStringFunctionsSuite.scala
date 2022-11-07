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

  final val LENGTH = 1000

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
    this.checkLengthAndPlan(df, LENGTH)
  }

  test("ascii") {
    runQueryAndCompare(s"select l_orderkey, ascii(l_comment) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }

    runQueryAndCompare(s"select l_orderkey, ascii(null) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }

  test("concat") {
    runQueryAndCompare(s"select l_orderkey, concat(l_comment, 'hello') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, concat(l_comment, 'hello', 'world') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }

  test("instr") {
    runQueryAndCompare(s"select l_orderkey, instr(l_comment, 'h') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, instr(l_comment, null) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, instr(null, 'h') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }

  test("length") {
    runQueryAndCompare(s"select l_orderkey, length(l_comment) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, length(null) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }

    runQueryAndCompare(s"select l_orderkey, CHAR_LENGTH(l_comment) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, CHAR_LENGTH(null) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }

    runQueryAndCompare(s"select l_orderkey, CHARACTER_LENGTH(l_comment) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, CHARACTER_LENGTH(null) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }

  test("lower") {
    runQueryAndCompare(s"select l_orderkey, lower(l_comment) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, lower(null) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }

  test("upper") {
    runQueryAndCompare(s"select l_orderkey, upper(l_comment) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, upper(null) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }

  test("lcase") {
    runQueryAndCompare(s"select l_orderkey, lcase(l_comment) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, lcase(null) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }

  test("ucase") {
    runQueryAndCompare(s"select l_orderkey, ucase(l_comment) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, ucase(null) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }

  ignore("locate") {
    runQueryAndCompare(s"select l_orderkey, locate(l_comment, 'a', 1) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, locate(null, 'a', 1) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }

  test("ltrim/rtrim") {
    runQueryAndCompare(s"select l_orderkey, ltrim('SparkSQL   ', 'Spark') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, ltrim('    SparkSQL   ', 'Spark') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, ltrim('    SparkSQL   ') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, ltrim(null) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, ltrim(l_comment) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }

    runQueryAndCompare(s"select l_orderkey, rtrim('    SparkSQL   ') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, rtrim(null) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, rtrim(l_comment) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }

  test("lpad") {
    runQueryAndCompare(s"select l_orderkey, lpad(null, 80) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, lpad(l_comment, 80) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, lpad(l_comment, 80, '??') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, lpad(l_comment, null, '??') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, lpad(l_comment, 80, null) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }

  test("rpad") {
    runQueryAndCompare(s"select l_orderkey, rpad(null, 80) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, rpad(l_comment, 80) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, rpad(l_comment, 80, '??') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, rpad(l_comment, null, '??') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, rpad(l_comment, 80, null) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }

  test("like") {
    runQueryAndCompare("""select l_orderkey, like(l_comment, '%\%') """ +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, like(l_comment, 'a_%b') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, like('l_comment', 'a\\__b') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, like(l_comment, 'abc_') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, like(l_comment, ' ') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, like(null, '%a%') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, l_comment " +
      s"from lineitem where l_comment like '%a%' limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, like(l_comment, ' ') " +
      s"from lineitem where l_comment like ''  limit $LENGTH") { _ => }
    runQueryAndCompare(s"select l_orderkey, like(null, '%a%') " +
      s"from lineitem where l_comment like '%$$##@@#&&' limit $LENGTH") { _ => }
  }

  test("rlike") {
    runQueryAndCompare(s"select l_orderkey, l_comment, rlike(l_comment, 'a*') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, rlike(l_comment, ' ') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, rlike(null, '%a%') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, l_comment " +
      s"from lineitem where l_comment rlike '%a%' limit $LENGTH") { _ => }
    runQueryAndCompare(s"select l_orderkey, like(l_comment, ' ') " +
      s"from lineitem where l_comment rlike ''  limit $LENGTH") { _ => }
    runQueryAndCompare(s"select l_orderkey, like(null, '%a%') " +
      s"from lineitem where l_comment rlike '%$$##@@#&&' limit $LENGTH") { _ => }
  }

  test("regexp") {
    runQueryAndCompare(s"select l_orderkey, l_comment, regexp(l_comment, 'a*') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, regexp(l_comment, ' ') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, regexp(null, '%a%') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, l_comment " +
      s"from lineitem where l_comment regexp '%a%' limit $LENGTH") { _ => }
    runQueryAndCompare(s"select l_orderkey, l_comment " +
      s"from lineitem where l_comment regexp ''  limit $LENGTH") { _ => }
    runQueryAndCompare(s"select l_orderkey, l_comment " +
      s"from lineitem where l_comment regexp '%$$##@@#&&' limit $LENGTH") { _ => }
  }

  test("regexp_like") {
    runQueryAndCompare(s"select l_orderkey, l_comment, regexp_like(l_comment, 'a*') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, regexp_like(l_comment, ' ') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, regexp_like(null, '%a%') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }

  test("regexp_extract") {
    runQueryAndCompare(s"select l_orderkey, regexp_extract(l_comment, '([a-z])', 1) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, regexp_extract(null, '([a-z])', 1) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }

  test("replace") {
    runQueryAndCompare(s"select l_orderkey, replace(l_comment, ' ', 'hello') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, replace(l_comment, 'ha') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, replace(l_comment, ' ', null) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, replace(l_comment, null, 'hello') " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }

  test("split") {
    val df = runQueryAndCompare(s"select l_orderkey, split(l_comment, 'h', 3) " +
      s"from lineitem limit $LENGTH") { _ => }
    assert(df.collect().length == LENGTH)
  }

  test("substr") {
    runQueryAndCompare(s"select l_orderkey, substr(l_comment, 1) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, substr(l_comment, 1, 3) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, substr(null, 1) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, substr(null, 1, 3) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, substr(l_comment, null) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, substr(l_comment, null, 3) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }

  test("substring") {
    runQueryAndCompare(s"select l_orderkey, substring(l_comment, 1) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, substring(l_comment, 1, 3) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, substring(null, 1) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, substring(null, 1, 3) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, substring(l_comment, null) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
    runQueryAndCompare(s"select l_orderkey, substring(l_comment, null, 3) " +
      s"from lineitem limit $LENGTH") { checkLengthAndPlan }
  }
}
