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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
 * A test suite that tests basic Parquet I/O.
 */
class GlutenParquetIOSuite extends ParquetIOSuite with GlutenSQLTestsBaseTrait {
  override protected def testFile(fileName: String): String = {
    getWorkspaceFilePath(
      "sql", "core", "src", "test", "resources").toString + "/" + fileName
  }

  override protected def readResourceParquetFile(name: String): DataFrame = {
    spark.read.parquet(testFile(name))
  }

  test(GlutenTestConstants.GLUTEN_TEST +
    "SPARK-35640: int as long should throw schema incompatible error") {
    val data = (1 to 4).map(i => Tuple1(i))
    val readSchema = StructType(Seq(StructField("_1", DataTypes.LongType)))

    withParquetFile(data) { path =>
      val errMsg = intercept[Exception](spark.read.schema(readSchema).parquet(path).collect())
        .getMessage
      assert(errMsg.contains(
        "BaseVector::compatibleKind(BaseVector::typeKind(), source->typeKind())"))
    }
  }
}
