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

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

class VeloxBloomFilterAggregateFunctionsSuite extends VeloxWholeStageTransformerSuite {
  val funcId_bloom_filter_agg = new FunctionIdentifier("bloom_filter_agg")
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"
  val table = "bloomTable"

  protected def registerFunAndcCreatTable(): Unit = {
    val funcId_bloom_filter_agg = new FunctionIdentifier("bloom_filter_agg")
    // Register 'bloom_filter_agg'
    spark.sessionState.functionRegistry.registerFunction(
      funcId_bloom_filter_agg,
      new ExpressionInfo(classOf[BloomFilterAggregate].getName, "bloom_filter_agg"),
      (children: Seq[Expression]) =>
        children.size match {
          case 1 => new BloomFilterAggregate(children.head)
          case 2 => new BloomFilterAggregate(children.head, children(1))
          case 3 => new BloomFilterAggregate(children.head, children(1), children(2))
        }
    )
    val schema2 = new StructType()
      .add("a2", IntegerType, nullable = true)
      .add("b2", LongType, nullable = true)
      .add("c2", IntegerType, nullable = true)
      .add("d2", IntegerType, nullable = true)
      .add("e2", IntegerType, nullable = true)
      .add("f2", IntegerType, nullable = true)
    val data2 = Seq(
      Seq(67, 17L, 45, 91, null, null),
      Seq(98, 63L, 0, 89, null, 40),
      Seq(null, null, 68, 75, 20, 19))
    val rdd2 = spark.sparkContext.parallelize(data2)
    val rddRow2 = rdd2.map(s => Row.fromSeq(s))
    spark.createDataFrame(rddRow2, schema2).write.saveAsTable(table)
  }

  protected def dropFunctionAndTable(): Unit = {
    spark.sessionState.functionRegistry.dropFunction(funcId_bloom_filter_agg)
    spark.sql(s"DROP TABLE IF EXISTS $table")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    registerFunAndcCreatTable()
  }

  override def afterAll(): Unit = {
    dropFunctionAndTable()
    super.afterAll()
  }

  test("Test bloom_filter_agg with Nulls input") {
    spark
      .sql(s"""
              |SELECT bloom_filter_agg(b2) from $table
         """.stripMargin)
      .show
  }
}
