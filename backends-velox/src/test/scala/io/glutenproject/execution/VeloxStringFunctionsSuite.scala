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

  test("ascii") {
    runQueryAndCompare(s"select l_orderkey, ascii(l_comment) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }

    runQueryAndCompare(s"select l_orderkey, ascii(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("concat") {
    runQueryAndCompare(s"select l_orderkey, concat(l_comment, 'hello') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, concat(l_comment, 'hello', 'world') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("extract") {
    runQueryAndCompare(s"select l_orderkey, l_shipdate, " +
      s"extract(doy FROM DATE'2019-08-12') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("day") {
    runQueryAndCompare(s"select l_orderkey, l_shipdate, day(l_shipdate) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, day(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("dayofmonth") {
    runQueryAndCompare(s"select l_orderkey, l_shipdate, dayofmonth(l_shipdate) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, dayofmonth(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("day_of_year") {
    runQueryAndCompare(s"select l_orderkey, l_shipdate, dayofyear(l_shipdate) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, dayofyear(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("dayofweek") {
    runQueryAndCompare(s"select l_orderkey, l_shipdate, dayofweek(l_shipdate) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, dayofweek(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  ignore("weekday") {// todo: result mismatched
    runQueryAndCompare(s"select l_orderkey, l_shipdate, weekday(l_shipdate) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, weekday(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("month") {
    runQueryAndCompare(s"select l_orderkey, l_shipdate, month(l_shipdate) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, month(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("quarter") {
    runQueryAndCompare(s"select l_orderkey, l_shipdate, quarter(l_shipdate) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, quarter(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("year") {
    runQueryAndCompare(s"select l_orderkey, l_shipdate, year(l_shipdate) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, year(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("instr") {
    runQueryAndCompare(s"select l_orderkey, instr(l_comment, 'h') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, instr(l_comment, null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, instr(null, 'h') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("length") {
    runQueryAndCompare(s"select l_orderkey, length(l_comment) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, length(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }

    runQueryAndCompare(s"select l_orderkey, CHAR_LENGTH(l_comment) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, CHAR_LENGTH(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }

    runQueryAndCompare(s"select l_orderkey, CHARACTER_LENGTH(l_comment) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, CHARACTER_LENGTH(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("md5") {
    runQueryAndCompare(s"select l_orderkey, md5(l_comment) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, md5(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("lower") {
    runQueryAndCompare(s"select l_orderkey, lower(l_comment) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, lower(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("upper") {
    runQueryAndCompare(s"select l_orderkey, upper(l_comment) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, upper(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("lcase") {
    runQueryAndCompare(s"select l_orderkey, lcase(l_comment) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, lcase(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("ucase") {
    runQueryAndCompare(s"select l_orderkey, ucase(l_comment) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, ucase(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  ignore("locate") {
    runQueryAndCompare(s"select l_orderkey, locate(l_comment, 'a', 1) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, locate(null, 'a', 1) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("trim") {
    runQueryAndCompare(s"select l_orderkey, trim('    SparkSQL   ') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, trim(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, trim(l_comment) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("ltrim/rtrim") {
    runQueryAndCompare(s"select l_orderkey, ltrim('SparkSQL   ', 'Spark') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, ltrim('    SparkSQL   ', 'Spark') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, ltrim('    SparkSQL   ') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, ltrim(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, ltrim(l_comment) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }

    runQueryAndCompare(s"select l_orderkey, rtrim('    SparkSQL   ') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, rtrim(null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, rtrim(l_comment) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("lpad") {
    runQueryAndCompare(s"select l_orderkey, lpad(null, 80) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, lpad(l_comment, 80) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, lpad(l_comment, 80, '??') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, lpad(l_comment, null, '??') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, lpad(l_comment, 80, null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("rpad") {
    runQueryAndCompare(s"select l_orderkey, rpad(null, 80) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, rpad(l_comment, 80) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, rpad(l_comment, 80, '??') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, rpad(l_comment, null, '??') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, rpad(l_comment, 80, null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("like") {
    runQueryAndCompare("""select l_orderkey, like(l_comment, '%\%') """ +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, like(l_comment, 'a_%b') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, like('l_comment', 'a\\__b') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, like(l_comment, 'abc_') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, like(l_comment, ' ') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, like(null, '%a%') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, l_comment " +
      s"from lineitem where l_comment like '%a%' limit $LENGTH") {
      checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, like(l_comment, ' ') " +
      s"from lineitem where l_comment like ''  limit $LENGTH") { _ => }
    runQueryAndCompare(s"select l_orderkey, like(null, '%a%') " +
      s"from lineitem where l_comment like '%$$##@@#&&' limit $LENGTH") { _ => }
  }

  test("rlike") {
    runQueryAndCompare(s"select l_orderkey, l_comment, rlike(l_comment, 'a*') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, rlike(l_comment, ' ') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, rlike(null, '%a%') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, l_comment " +
      s"from lineitem where l_comment rlike '%a%' limit $LENGTH") { _ => }
    runQueryAndCompare(s"select l_orderkey, like(l_comment, ' ') " +
      s"from lineitem where l_comment rlike ''  limit $LENGTH") { _ => }
    runQueryAndCompare(s"select l_orderkey, like(null, '%a%') " +
      s"from lineitem where l_comment rlike '%$$##@@#&&' limit $LENGTH") { _ => }
  }

  test("regexp") {
    runQueryAndCompare(s"select l_orderkey, l_comment, regexp(l_comment, 'a*') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, regexp(l_comment, ' ') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, regexp(null, '%a%') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, l_comment " +
      s"from lineitem where l_comment regexp '%a%' limit $LENGTH") { _ => }
    runQueryAndCompare(s"select l_orderkey, l_comment " +
      s"from lineitem where l_comment regexp ''  limit $LENGTH") { _ => }
    runQueryAndCompare(s"select l_orderkey, l_comment " +
      s"from lineitem where l_comment regexp '%$$##@@#&&' limit $LENGTH") { _ => }
  }

  test("regexp_like") {
    runQueryAndCompare(s"select l_orderkey, l_comment, regexp_like(l_comment, 'a*') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, regexp_like(l_comment, ' ') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, regexp_like(null, '%a%') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("regexp_extract") {
    runQueryAndCompare(s"select l_orderkey, regexp_extract(l_comment, '([a-z])', 1) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, regexp_extract(null, '([a-z])', 1) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("regexp_extract_all") {
    runQueryAndCompare("select l_orderkey, regexp_extract_all('l_comment', '([a-z])', 1) " +
      "from lineitem limit 5") { checkOperatorMatch[ProjectExecTransformer] }
    // fall back because of unsupported cast(array)
    runQueryAndCompare("select l_orderkey, l_comment, " +
      "regexp_extract_all(l_comment, '([a-z]+)', 0) " +
      "from lineitem limit 5") { _ => }
  }

  ignore("regexp_replace") {
    runQueryAndCompare("select l_orderkey, regexp_replace(l_comment, '([a-z])', '1') " +
      "from lineitem limit 5") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare("select l_orderkey, regexp_replace(l_comment, '([a-z])', '1', 1) " +
      "from lineitem limit 5") { checkOperatorMatch[ProjectExecTransformer] }
    // todo incorrect results
    runQueryAndCompare("select l_orderkey, regexp_replace(l_comment, '([a-z])', '1', 10) " +
      "from lineitem limit 5") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("replace") {
    runQueryAndCompare(s"select l_orderkey, replace(l_comment, ' ', 'hello') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, replace(l_comment, 'ha') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, replace(l_comment, ' ', null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, replace(l_comment, null, 'hello') " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("reverse") {
    runQueryAndCompare("select l_orderkey, l_comment, reverse(l_comment) " +
      "from lineitem limit 5") { checkOperatorMatch[ProjectExecTransformer] }

    // fall back because of unsupported cast(array)
    runQueryAndCompare("select l_orderkey, l_comment, reverse(array(l_comment, l_comment)) " +
      "from lineitem limit 5") { _ => }
  }

  ignore("split") {
    runQueryAndCompare("select l_orderkey, l_comment, split(l_comment, ' ', 3) " +
          "from lineitem limit 5") { _ => }

    // todo incorrect results
    runQueryAndCompare("select l_orderkey, l_comment, split(l_comment, '[a]', 3) " +
      "from lineitem limit 5") { _ => }

    runQueryAndCompare("select l_orderkey, split(l_comment, ' ') " +
      "from lineitem limit 5") { _ => }

    runQueryAndCompare("select l_orderkey, split(l_comment, 'h') " +
      "from lineitem limit 5") { _ => }
  }

  test("substr") {
    runQueryAndCompare(s"select l_orderkey, substr(l_comment, 1) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, substr(l_comment, 1, 3) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, substr(null, 1) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, substr(null, 1, 3) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, substr(l_comment, null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, substr(l_comment, null, 3) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }

  test("substring") {
    runQueryAndCompare(s"select l_orderkey, substring(l_comment, 1) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, substring(l_comment, 1, 3) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, substring(null, 1) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, substring(null, 1, 3) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, substring(l_comment, null) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
    runQueryAndCompare(s"select l_orderkey, substring(l_comment, null, 3) " +
      s"from lineitem limit $LENGTH") { checkOperatorMatch[ProjectExecTransformer] }
  }
}
