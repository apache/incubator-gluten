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
package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.GlutenImplicits._
import org.apache.spark.sql.internal.SQLConf

class GlutenImplicitsTest extends GlutenSQLTestsBaseTrait {
  sys.props.put("spark.gluten.sql.columnar.tableCache", "true")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark
      .range(10)
      .selectExpr("id as c1", "id % 3 as c2")
      .write
      .format("parquet")
      .saveAsTable("t1")
  }

  override protected def afterAll(): Unit = {
    spark.sql("drop table t1")
    super.afterAll()
  }

  override protected def afterEach(): Unit = {
    spark.catalog.clearCache()
    super.afterEach()
  }

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.shuffle.partitions", "5")
  }

  private def withAQEEnabledAndDisabled(f: => Unit): Unit = {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "true",
      SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING.key -> "true"
    ) {
      f
    }
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false",
      SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> "false",
      SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING.key -> "false"
    ) {
      f
    }
  }

  testGluten("fallbackSummary with query") {
    withAQEEnabledAndDisabled {
      val df = spark.table("t1").filter(_.getLong(0) > 0)
      assert(df.fallbackSummary().numGlutenNodes == 1, df.fallbackSummary())
      assert(df.fallbackSummary().numFallbackNodes == 1, df.fallbackSummary())
      df.collect()
      assert(df.fallbackSummary().numGlutenNodes == 1, df.fallbackSummary())
      assert(df.fallbackSummary().numFallbackNodes == 1, df.fallbackSummary())
    }
  }

  testGluten("fallbackSummary with shuffle") {
    withAQEEnabledAndDisabled {
      val df = spark.sql("SELECT c2 FROM t1 group by c2").filter(_.getLong(0) > 0)
      assert(df.fallbackSummary().numGlutenNodes == 6, df.fallbackSummary())
      assert(df.fallbackSummary().numFallbackNodes == 1, df.fallbackSummary())
      df.collect()
      assert(df.fallbackSummary().numGlutenNodes == 6, df.fallbackSummary())
      assert(df.fallbackSummary().numFallbackNodes == 1, df.fallbackSummary())
    }
  }

  testGluten("fallbackSummary with set command") {
    withAQEEnabledAndDisabled {
      val df = spark.sql("set k=v")
      assert(df.fallbackSummary().numGlutenNodes == 0, df.fallbackSummary())
      assert(df.fallbackSummary().numFallbackNodes == 0, df.fallbackSummary())
    }
  }

  testGluten("fallbackSummary with data write command") {
    withAQEEnabledAndDisabled {
      withTable("tmp") {
        val df = spark.sql("create table tmp using parquet as select * from t1")
        assert(df.fallbackSummary().numGlutenNodes == 1, df.fallbackSummary())
        assert(df.fallbackSummary().numFallbackNodes == 0, df.fallbackSummary())
      }
    }
  }

  testGluten("fallbackSummary with cache") {
    withAQEEnabledAndDisabled {
      val df = spark.table("t1").cache().filter(_.getLong(0) > 0)
      assert(df.fallbackSummary().numGlutenNodes == 2, df.fallbackSummary())
      assert(df.fallbackSummary().numFallbackNodes == 1, df.fallbackSummary())
      df.collect()
      assert(df.fallbackSummary().numGlutenNodes == 2, df.fallbackSummary())
      assert(df.fallbackSummary().numFallbackNodes == 1, df.fallbackSummary())
    }
  }

  testGluten("fallbackSummary with cached data and shuffle") {
    withAQEEnabledAndDisabled {
      val df = spark.sql("select * from t1").filter(_.getLong(0) > 0).cache.repartition()
      assert(df.fallbackSummary().numGlutenNodes == 7, df.fallbackSummary())
      assert(df.fallbackSummary().numFallbackNodes == 1, df.fallbackSummary())
      df.collect()
      assert(df.fallbackSummary().numGlutenNodes == 7, df.fallbackSummary())
      assert(df.fallbackSummary().numFallbackNodes == 1, df.fallbackSummary())
    }
  }
}
