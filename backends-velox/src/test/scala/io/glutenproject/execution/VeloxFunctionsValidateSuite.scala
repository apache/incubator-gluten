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

import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.optimizer.{ConstantFolding, NullPropagation}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, expr}

import scala.collection.JavaConverters._

class VeloxFunctionsValidateSuite extends WholeStageTransformerSuite {

  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"
  override protected val backend: String = "velox"

  private var parquetPath: String = _

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.sql.files.maxPartitionBytes", "1g")
      .set("spark.sql.shuffle.partitions", "1")
      .set("spark.memory.offHeap.size", "2g")
      .set("spark.unsafe.exceptionOnMemoryLeak", "false")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.optimizer.excludedRules", ConstantFolding.ruleName + "," +
        NullPropagation.ruleName)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHNotNullTables()

    val lfile = Files.createTempFile("", ".parquet").toFile
    lfile.deleteOnExit()
    parquetPath = lfile.getAbsolutePath

    val schema = StructType(Array(
      StructField("double_field1", DoubleType, true)
    ))
    val rowData = Seq(
      Row(1.025),
      Row(1.035),
      Row(1.045)
    )

    var dfParquet = spark.createDataFrame(rowData.asJava, schema)
    dfParquet.coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .parquet(parquetPath)

    spark.catalog.createTable("datatab", parquetPath, fileFormat)
  }

  test("Test acos function") {
    val df = runQueryAndCompare("SELECT acos(l_orderkey) from lineitem limit 1") { _ => }
    df.show()
    df.explain(false)
    df.printSchema()
    checkLengthAndPlan(df, 1)
  }

  test("Test asin function") {
    val df = runQueryAndCompare("SELECT asin(l_orderkey) from lineitem limit 1") { _ => }
    df.show()
    df.explain(false)
    df.printSchema()
    checkLengthAndPlan(df, 1)
  }

  test("Test atan function") {
    val df = runQueryAndCompare("SELECT atan(l_orderkey) from lineitem limit 1") { _ => }
    df.show()
    df.explain(false)
    df.printSchema()
    checkLengthAndPlan(df, 1)
  }

  ignore("Test atan2 function datatab") {
    val df = runQueryAndCompare("SELECT atan2(double_field1, 0) from " +
      "datatab limit 1") { _ => }
    df.show()
    df.explain(false)
    df.printSchema()
    assert(df.queryExecution.executedPlan.find(_.isInstanceOf[ProjectExecTransformer]).isDefined)
  }

  test("Test ceiling function") {
    val df = runQueryAndCompare("SELECT ceiling(cast(l_orderkey as long)) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test cos function") {
    val df = runQueryAndCompare("SELECT cos(l_orderkey) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test cosh function") {
    val df = runQueryAndCompare("SELECT cosh(l_orderkey) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test degrees function") {
    val df = runQueryAndCompare("SELECT degrees(l_orderkey) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
    df.show()
    df.explain(false)
    df.printSchema()
  }

  test("Test log10 function") {
    val df = runQueryAndCompare("SELECT log10(l_orderkey) " +
      "from lineitem limit 1") { _ => }
    checkLengthAndPlan(df, 1)
    df.show()
    df.explain(false)
    df.printSchema()
  }
}
