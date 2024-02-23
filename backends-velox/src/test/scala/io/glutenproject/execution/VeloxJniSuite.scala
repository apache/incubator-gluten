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

import io.glutenproject.vectorized.JniWorkspace

import org.apache.spark.SparkConf

import org.scalatest.PrivateMethodTester

class VeloxJniSuite extends VeloxWholeStageTransformerSuite with PrivateMethodTester {
  override protected val backend: String = "velox"
  override protected val resourcePath: String = "/tpch-data-parquet-velox"
  override protected val fileFormat: String = "parquet"

  private val resetDefaultInstance = {
    val out = classOf[JniWorkspace].getDeclaredMethod("resetDefaultInstance")
    out.setAccessible(true)
    out
  }

  private val isDebugEnabled = {
    val out = classOf[JniWorkspace].getDeclaredMethod("isDebugEnabled")
    out.setAccessible(true)
    out
  }

  override def beforeAll(): Unit = {
    resetDefaultInstance.invoke(null)
    super.beforeAll()
    createTPCHNotNullTables()
  }

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.gluten.sql.debug", "true")
      .set("spark.gluten.sql.debug.keepJniWorkspace", "true")
  }

  test("assert debug enabled") {
    val df = spark
      .sql("select * from lineitem limit 1")
    df.collect()
    assert(isDebugEnabled.invoke(null).asInstanceOf[Boolean] === true)
  }
}
