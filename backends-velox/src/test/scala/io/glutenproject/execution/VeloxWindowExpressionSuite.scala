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
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{CurrentRow, RowFrame, RowNumber, SpecifiedWindowFrame, UnboundedPreceding}
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation

class VeloxWindowExpressionSuite extends WholeStageTransformerSuite {

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
      .set("spark.unsafe.exceptionOnMemoryLeak", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.sources.useV1SourceList", "avro")
      .set("spark.sql.windowFunc.forceOrder", "false")
  }

  private def checkOperator(df: DataFrame)(customCheck: DataFrame => Unit): Seq[Row] = {
    val result = df.collect()
    customCheck(df)
    result
  }

  test("Window expression") {
    val originalQuery = spark
      .sql(
        "select row_number() over" +
          " (partition by l_suppkey order by l_orderkey) from lineitem ")
      .toDF()
    checkOperator(originalQuery)(checkOperatorMatch[WindowExecTransformer])
  }

  test("Window expression to AggregateExpression") {
    val r = LocalRelation.fromExternalRows(
      Seq("a".attr.int, "b".attr.int, "c".attr.int),
      1.to(6).map(_ => Row(1, 2, 3)))
    val a = r.output(0)
    val b = r.output(1)
    val c = r.output(2)
    val windowFrame = SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow)

    val originalQuery = r
      .select(
        a,
        b,
        c,
        windowExpr(
          RowNumber().toAggregateExpression(),
          windowSpec(a :: Nil, c.desc :: Nil, windowFrame)).as("rn"))
      .limit(2)
    checkOperator(originalQuery)(checkOperatorMatch[WindowExecTransformer])
  }
}
