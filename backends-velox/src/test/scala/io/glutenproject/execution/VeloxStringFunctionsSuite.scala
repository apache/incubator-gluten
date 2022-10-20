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
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, TestUtils}

import scala.collection.JavaConverters

class VeloxStringFunctionsSuite extends WholeStageTransformerSuite {

  protected val rootPath: String = getClass.getResource("/").getPath
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-orc-velox"
  override protected val fileFormat: String = "orc"

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
    var result = runSql("select l_orderkey, ascii(l_comment) " +
      "from lineitem limit 5") { _ => }
    assert(result.length == 5)
    result = runSql("select l_orderkey, ascii(null) " +
      "from lineitem limit 5") { _ => }
    assert(result.length == 5)
  }

  test("concat") {
    var result = runSql("select l_orderkey, concat(l_comment, 'hello') " +
      "from lineitem limit 5") { _ => }
    assert(result.length == 5)
    result = runSql("select l_orderkey, concat(l_comment, 'hello', 'world') " +
      "from lineitem limit 5") { _ => }
    assert(result.length == 5)
  }

  test("instr") {
    var result = runSql("select l_orderkey, instr(l_comment, 'h') from lineitem limit 5") { _ => }
    assert(result.length == 5)
    result = runSql("select l_orderkey, instr(l_comment, null) from lineitem limit 5") { _ => }
    assert(result.length == 5)
    result = runSql("select l_orderkey, instr(null, 'h') from lineitem limit 5") { _ => }
    assert(result.length == 5)
  }

  test("length") {
    var result = runSql("select l_orderkey, length(l_comment) from lineitem limit 5") { _ => }
    assert(result.length == 5)
    result = runSql("select l_orderkey, length(null) from lineitem limit 5") { _ => }
    assert(result.length == 5)
  }

  test("lower") {
    var result = runSql("select l_orderkey, lower(l_comment) from lineitem limit 5") { _ => }
    assert(result.length == 5)
    result = runSql("select l_orderkey, lower(null) from lineitem limit 5") { _ => }
    assert(result.length == 5)
  }

  test("upper") {
    var result = runSql("select l_orderkey, upper(l_comment) from lineitem limit 5") { _ => }
    assert(result.length == 5)
    result = runSql("select l_orderkey, upper(null) from lineitem limit 5") { _ => }
    assert(result.length == 5)
  }

  test("replace") {
    var result = runSql("select l_orderkey, replace(l_comment, ' ', 'hello') " +
      "from lineitem limit 5") { _ => }
    assert(result.length == 5)
    result = runSql("select l_orderkey, replace(l_comment, 'ha') " +
      "from lineitem limit 5") { _ => }
    assert(result.length == 5)
    result = runSql("select l_orderkey, replace(l_comment, ' ', null) " +
      "from lineitem limit 5") { _ => }
    assert(result.length == 5)
    result = runSql("select l_orderkey, replace(l_comment, null, 'hello') " +
      "from lineitem limit 5") { _ => }
    assert(result.length == 5)
  }

  test("split") {
    val result = runSql("select l_orderkey, split(l_comment, 'h', 3) " +
      "from lineitem limit 5") { _ => }
    assert(result.length == 5)
  }

}
