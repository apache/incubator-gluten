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

package io.glutenproject.benchmarks

import io.glutenproject.execution.{WholeStageTransformerExec, WholeStageTransformerSuite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec

import org.apache.commons.io.FileUtils

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.collection.JavaConverters._

class NativeBenchmarkPlanGenerator extends WholeStageTransformerSuite {
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"
  val generatedPlanDir = getClass.getResource("/").getPath + "../../../generated-native-benchmark/"
  val outputFileFormat = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val dir = new File(generatedPlanDir)
    if (dir.exists()) {
      FileUtils.forceDelete(dir)
    }
    FileUtils.forceMkdir(dir)
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }

  test("Generate example substrait plan") {
    val q4_lineitem = spark
      .sql(s"""
              |select l_orderkey from lineitem where l_commitdate < l_receiptdate
              |""".stripMargin)
    val q4_orders = spark
      .sql(s"""
              |select o_orderkey, o_orderpriority
              |  from orders
              |  where o_orderdate >= '1993-07-01' and o_orderdate < '1993-10-01'
              |""".stripMargin)
    q4_lineitem
      .createOrReplaceTempView("q4_lineitem")
    q4_orders
      .createOrReplaceTempView("q4_orders")
    q4_lineitem.write.format(outputFileFormat).save(generatedPlanDir + "/example_lineitem")
    q4_orders.write.format(outputFileFormat).save(generatedPlanDir + "/example_orders")

    val df =
      spark.sql("""
                  |select * from q4_orders left semi join q4_lineitem on l_orderkey = o_orderkey
                  |""".stripMargin)
    df.collect()

    val executedPlan = df.queryExecution.executedPlan
    assert(executedPlan.isInstanceOf[AdaptiveSparkPlanExec])
    val finalPlan = executedPlan.asInstanceOf[AdaptiveSparkPlanExec].executedPlan
    val lastStageTransformer = finalPlan.find(_.isInstanceOf[WholeStageTransformerExec])
    assert(lastStageTransformer.nonEmpty)
    val plan = lastStageTransformer.get.asInstanceOf[WholeStageTransformerExec].planJson.split('\n')

    val exampleJsonFile = Paths.get(generatedPlanDir, "example.json")
    Files.write(exampleJsonFile, plan.toList.asJava, StandardCharsets.UTF_8)
  }
}
