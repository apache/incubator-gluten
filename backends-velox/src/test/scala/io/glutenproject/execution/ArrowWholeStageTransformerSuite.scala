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

class ArrowWholeStageTransformerSuite extends WholeStageTransformerSuite {
  override protected val backend: String = "gazelle_cpp"
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHTables()
  }

  test("test scan non-fallback") {
    val df = spark.sql("select * from lineitem")
    val plan = df.queryExecution.executedPlan
    val transformers = plan.collect {
      case t : TransformSupport => t
    }
    assert(transformers.length == 2)
    df.count()
  }

  test("test scan-filter non-fallback") {
    val df = spark.sql("select max(l_partkey) from lineitem where l_partkey is not null")
    val plan = df.queryExecution.executedPlan
    val transformers = plan.collect {
      case t : TransformSupport => t
    }
    assert(transformers.length == 3)
  }
}
