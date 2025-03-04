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
package org.apache.gluten.execution

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class VeloxOrcSchemaEvolutionSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  import testImplicits._

  test("read ORC with column names all starting with '_col'") {
    withTempPath {
      tmp =>
        val df = Seq((1, 2, 3), (4, 5, 6), (7, 8, 9)).toDF("_col0", "_col1", "_col2")
        df.write.format("orc").save(s"file://${tmp.getCanonicalPath}")

        withTempView("test") {
          spark.read
            .format("orc")
            .schema(
              StructType(
                Array(
                  StructField("a", IntegerType, nullable = true),
                  StructField("b", IntegerType, nullable = true),
                  StructField("c", IntegerType, nullable = true)
                )))
            .load(s"file://${tmp.getCanonicalPath}")
            .createOrReplaceTempView("test")

          runQueryAndCompare("select a, b, c from test") {
            df =>
              checkAnswer(
                df,
                Row(1, 2, 3) ::
                  Row(4, 5, 6) ::
                  Row(7, 8, 9) ::
                  Nil)
          }
        }
    }
  }
}
