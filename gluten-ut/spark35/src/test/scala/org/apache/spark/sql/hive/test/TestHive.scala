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
package org.apache.spark.sql.hive.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.config
import org.apache.spark.internal.config.UI._
import org.apache.spark.sql.GlutenSQLTestsBaseTrait
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.optimizer.ConvertToLocalRelation
import org.apache.spark.sql.hive._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH

/**
 * Object TestHive is singleton and hard to modify sparkConf in it, so copy from spark source code
 * and add gluten related conf here. This class is expected to overwrite the one from spark.
 */
object TestHive
  extends TestHiveContext(
    new SparkContext(
      System.getProperty("spark.sql.test.master", "local[1]"),
      "TestSQLContext",
      GlutenSQLTestsBaseTrait.nativeSparkConf(
        new SparkConf()
          .set("spark.sql.test", "")
          .set(SQLConf.CODEGEN_FALLBACK.key, "false")
          .set(SQLConf.CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.CODEGEN_ONLY.toString)
          .set(
            HiveUtils.HIVE_METASTORE_BARRIER_PREFIXES.key,
            "org.apache.spark.sql.hive.execution.PairSerDe")
          .set(WAREHOUSE_PATH.key, TestHiveContext.makeWarehouseDir().toURI.getPath)
          // SPARK-8910
          .set(UI_ENABLED, false)
          .set(config.UNSAFE_EXCEPTION_ON_MEMORY_LEAK, true)
          // Hive changed the default of hive.metastore.disallow.incompatible.col.type.changes
          // from false to true. For details, see the JIRA HIVE-12320 and HIVE-17764.
          .set("spark.hadoop.hive.metastore.disallow.incompatible.col.type.changes", "false")
          // Disable ConvertToLocalRelation for better test coverage. Test cases built on
          // LocalRelation will exercise the optimization rules better by disabling it as
          // this rule may potentially block testing of other optimization rules such as
          // ConstantPropagation etc.
          .set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key, ConvertToLocalRelation.ruleName),
        TestHiveContext.makeWarehouseDir().toURI.getPath
      )
    ))
