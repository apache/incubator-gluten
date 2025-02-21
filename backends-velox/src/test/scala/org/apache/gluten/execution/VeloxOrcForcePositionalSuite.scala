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

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class VeloxOrcForcePositionalSuite extends VeloxWholeStageTransformerSuite {
  override protected val resourcePath: String = "/tpch-data-parquet"
  override protected val fileFormat: String = "parquet"

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.gluten.sql.columnar.backend.velox.glogSeverityLevel", "0")
      .set("spark.gluten.sql.columnar.backend.velox.glogVerboseLevel", "1")
      .set("spark.executorEnv.GLOG_v", "1")
      .set("spark.gluten.sql.debug", "true")
      .set("orc.force.positional.evolution", "true")

  import testImplicits._

  test("rename root columns") {
    withTempPath {
      tmp =>
        val path = tmp.getCanonicalPath
        val df1 = Seq((1, 2), (4, 5), (8, 9)).toDF("col1", "col2")
        val df2 = Seq((10, 12), (15, 19), (22, 40)).toDF("col1", "col3")

        val dir1 = s"file://$path/part=one"
        val dir2 = s"file://$path/part=two"

        df1.write.format("orc").save(dir1)
        df2.write.format("orc").save(dir2)

        spark.read
          .schema(df2.schema)
          .format("orc")
          .load(s"file://$path")
          .createOrReplaceTempView("test")

        runQueryAndCompare("select * from test") { _ => }
    }
  }

  test("rename nested columns") {
    withTempPath {
      tmp =>
        val path = tmp.getCanonicalPath
        val df1 = spark.createDataFrame(
          spark.sparkContext.parallelize(Row(1, Row("abc", 2)) :: Nil),
          schema = StructType(
            Array(
              StructField("col1", IntegerType, nullable = true),
              StructField(
                "col2",
                StructType(
                  Array(
                    StructField("a", StringType, nullable = true),
                    StructField("b", IntegerType, nullable = true)
                  )))
            ))
        )
        val df2 = spark.createDataFrame(
          spark.sparkContext.parallelize(Row(20, Row("EFG", 10)) :: Nil),
          schema = StructType(
            Array(
              StructField("col1", IntegerType, nullable = true),
              StructField(
                "col2",
                StructType(
                  Array(
                    StructField("a", StringType, nullable = true),
                    StructField("c", IntegerType, nullable = true)
                  )))
            ))
        )

        df1.write.mode("overwrite").format("orc").save(s"file://$path/part=one")
        df2.write.mode("overwrite").format("orc").save(s"file://$path/part=two")

        spark.read
          .schema(df2.schema)
          .format("orc")
          .load(s"file://$path")
          .createOrReplaceTempView("test")

        runQueryAndCompare("select * from test") { _ => }
    }
  }

  test("prune nested schema") {
    withTempPath {
      tmp =>
        val path = tmp.getCanonicalPath

        val df1 = spark.createDataFrame(
          spark.sparkContext.parallelize(Row(1, 5, Row("abc", 2)) :: Nil),
          schema = StructType(
            Array(
              StructField("col1", IntegerType, nullable = true),
              StructField("col2", IntegerType, nullable = true),
              StructField(
                "col3",
                StructType(
                  Array(
                    StructField("a", StringType, nullable = true),
                    StructField("b", IntegerType, nullable = true)
                  )))
            ))
        )
        df1.write.format("orc").save(s"file://$path")

        spark.read
          .format("orc")
          .schema(StructType(Array(
            StructField("col1", IntegerType, nullable = true),
            StructField("col2", IntegerType, nullable = true),
            StructField(
              "col3",
              StructType(Array(
                StructField("b", IntegerType, nullable = true)
              )))
          )))
          .load(s"file://$path")
          .createOrReplaceTempView("test")

        runQueryAndCompare("select * from test") { _ => }
    }
  }
}

class VeloxOrcForcePositionalOffSuite extends VeloxOrcForcePositionalSuite {
  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("orc.force.positional.evolution", "false")
}
