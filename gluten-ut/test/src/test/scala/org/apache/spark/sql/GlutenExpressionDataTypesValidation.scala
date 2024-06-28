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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.execution.{ProjectExecTransformer, WholeStageTransformerSuite}
import org.apache.gluten.utils.{BackendTestUtils, SystemParameters}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

class GlutenExpressionDataTypesValidation extends WholeStageTransformerSuite {
  protected val resourcePath: String = null
  protected val fileFormat: String = null

  import testImplicits._

  override protected def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
      .set("spark.ui.enabled", "false")
      .set("spark.gluten.ui.enabled", "false")
    if (BackendTestUtils.isCHBackendLoaded()) {
      conf
        .set("spark.gluten.sql.enable.native.validation", "false")
        .set(GlutenConfig.GLUTEN_LIB_PATH, SystemParameters.getClickHouseLibPath)
    }
    conf
  }

  def doTest(sql: String): Unit = {
    withTempPath {
      path =>
        Seq[String]("abc", null, "123", "aaa", "bbb", "ccc", "ddd", "a", "b", "c", null)
          .toDF("col")
          .write
          .parquet(path.getCanonicalPath)
        spark.read.parquet(path.getCanonicalPath).createOrReplaceTempView("tbl")
        runQueryAndCompare(sql, false) {
          checkGlutenOperatorMatch[ProjectExecTransformer]
        }
    }
  }

  def generateSimpleSQL(functionName: String): String = {
    String.format("SELECT %s(col) from tbl", functionName)
  }

  test("ascii") {
    val inputTypes = Size(null).inputTypes
    Ascii(null).prettyName
    if (inputTypes.size == 1 && inputTypes.head.acceptsType(StringType)) {
      doTest(generateSimpleSQL(Size(null).prettyName))
    }
  }
}
