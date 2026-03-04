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

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.DecimalType.{ByteDecimal, IntDecimal, LongDecimal, ShortDecimal}

import org.apache.parquet.hadoop.ParquetOutputFormat

class GlutenParquetTypeWideningSuite extends ParquetTypeWideningSuite with GlutenSQLTestsTrait {

  import testImplicits._

  // Disable native writer so that writeParquetFiles() uses Spark's Parquet writer.
  // This suite tests the READ path (type widening during reads). The native writer
  // doesn't produce DELTA_BINARY_PACKED/DELTA_BYTE_ARRAY encodings that the parent
  // test's V2 encoding assertions expect.
  override def sparkConf: SparkConf =
    super.sparkConf.set(GlutenConfig.NATIVE_WRITER_ENABLED.key, "false")

  // Velox always uses native reader (equivalent to Spark's vectorized reader).
  // For INT->Decimal with insufficient precision, Spark's vectorized reader rejects them
  // while parquet-mr allows them. Velox now rejects them (matching vectorized reader).
  // Override to set expectError = true for both reader config settings.
  for {
    (values, fromType, toType) <- Seq(
      (Seq("1", "2"), ByteType, DecimalType(1, 0)),
      (Seq("1", "2"), ByteType, ByteDecimal),
      (Seq("1", "2"), ShortType, ByteDecimal),
      (Seq("1", "2"), ShortType, ShortDecimal),
      (Seq("1", "2"), IntegerType, ShortDecimal),
      (Seq("1", "2"), ByteType, DecimalType(ByteDecimal.precision + 1, 1)),
      (Seq("1", "2"), ShortType, DecimalType(ShortDecimal.precision + 1, 1)),
      (Seq("1", "2"), LongType, IntDecimal),
      (Seq("1", "2"), ByteType, DecimalType(ByteDecimal.precision - 1, 0)),
      (Seq("1", "2"), ShortType, DecimalType(ShortDecimal.precision - 1, 0)),
      (Seq("1", "2"), IntegerType, DecimalType(IntDecimal.precision - 1, 0)),
      (Seq("1", "2"), LongType, DecimalType(LongDecimal.precision - 1, 0)),
      (Seq("1", "2"), ByteType, DecimalType(ByteDecimal.precision, 1)),
      (Seq("1", "2"), ShortType, DecimalType(ShortDecimal.precision, 1)),
      (Seq("1", "2"), IntegerType, DecimalType(IntDecimal.precision, 1)),
      (Seq("1", "2"), LongType, DecimalType(LongDecimal.precision, 1))
    )
  }
    testGluten(s"unsupported parquet conversion $fromType -> $toType") {
      for (dictionaryEnabled <- Seq(true, false)) {
        withClue(s"with dictionary encoding '$dictionaryEnabled'") {
          withAllParquetWriters {
            withTempDir {
              dir =>
                val df = values.toDF("a").select(col("a").cast(fromType))
                withSQLConf(ParquetOutputFormat.ENABLE_DICTIONARY -> dictionaryEnabled.toString) {
                  df.write.mode("overwrite").parquet(dir.getAbsolutePath)
                }
                withAllParquetReaders {
                  val exception = intercept[SparkException] {
                    spark.read.schema(s"a ${toType.sql}").parquet(dir.getAbsolutePath).collect()
                  }
                  assert(
                    exception.getCause
                      .isInstanceOf[SchemaColumnConvertNotSupportedException] ||
                      exception.getCause.getMessage.contains("not allowed for requested type"))
                }
            }
          }
        }
      }
    }

  // Velox rejects Decimal->Decimal narrowing (matching Spark vectorized reader behavior).
  // Override to set expectError = true for both reader configs.
  for {
    (fromPrecision, toPrecision) <-
      // Narrowing precision (same scale=2): Velox rejects like vectorized reader.
      Seq(7 -> 5, 10 -> 5, 20 -> 5, 12 -> 10, 20 -> 10, 22 -> 20)
  }
    testGluten(
      s"parquet decimal precision change Decimal($fromPrecision, 2) -> Decimal($toPrecision, 2)"
    ) {
      for (dictionaryEnabled <- Seq(true, false)) {
        withClue(s"with dictionary encoding '$dictionaryEnabled'") {
          withAllParquetWriters {
            withTempDir {
              dir =>
                val df = Seq("1.23", "10.34")
                  .toDF("a")
                  .select(col("a").cast(DecimalType(fromPrecision, 2)))
                withSQLConf(ParquetOutputFormat.ENABLE_DICTIONARY -> dictionaryEnabled.toString) {
                  df.write.mode("overwrite").parquet(dir.getAbsolutePath)
                }
                withAllParquetReaders {
                  val exception = intercept[SparkException] {
                    spark.read
                      .schema(s"a ${DecimalType(toPrecision, 2).sql}")
                      .parquet(dir.getAbsolutePath)
                      .collect()
                  }
                  assert(
                    exception.getCause
                      .isInstanceOf[SchemaColumnConvertNotSupportedException] ||
                      exception.getCause.getMessage.contains("not allowed for requested type"))
                }
            }
          }
        }
      }
    }

  // Velox rejects Decimal->Decimal scale narrowing and mixed scale changes
  // (convertType() enforces scaleIncrease >= 0 && precisionIncrease >= scaleIncrease).
  // Override to set expectError = true for both reader configs.
  for {
    ((fromPrecision, fromScale), (toPrecision, toScale)) <-
    // Narrowing precision and scale by the same amount.
    Seq(
      (7, 4) -> (5, 2),
      (10, 7) -> (5, 2),
      (20, 17) -> (5, 2),
      (12, 4) -> (10, 2),
      (20, 17) -> (10, 2),
      (22, 4) -> (20, 2)) ++
      // Increasing precision and decreasing scale.
      Seq((10, 6) -> (12, 4), (20, 7) -> (22, 5)) ++
      // Decreasing precision and increasing scale.
      Seq((12, 4) -> (10, 6), (22, 5) -> (20, 7)) ++
      // Increasing precision by a smaller amount than scale.
      Seq((5, 2) -> (6, 4), (10, 4) -> (12, 7), (20, 5) -> (22, 8))
  }
    testGluten(
      s"parquet decimal precision and scale change " +
        s"Decimal($fromPrecision, $fromScale) -> Decimal($toPrecision, $toScale)"
    ) {
      for (dictionaryEnabled <- Seq(true, false)) {
        withClue(s"with dictionary encoding '$dictionaryEnabled'") {
          withAllParquetWriters {
            withTempDir {
              dir =>
                val df = Seq("1.23", "10.34")
                  .toDF("a")
                  .select(col("a").cast(DecimalType(fromPrecision, fromScale)))
                withSQLConf(ParquetOutputFormat.ENABLE_DICTIONARY -> dictionaryEnabled.toString) {
                  df.write.mode("overwrite").parquet(dir.getAbsolutePath)
                }
                withAllParquetReaders {
                  val exception = intercept[SparkException] {
                    spark.read
                      .schema(s"a ${DecimalType(toPrecision, toScale).sql}")
                      .parquet(dir.getAbsolutePath)
                      .collect()
                  }
                  assert(
                    exception.getCause
                      .isInstanceOf[SchemaColumnConvertNotSupportedException] ||
                      exception.getCause.getMessage.contains("not allowed for requested type"))
                }
            }
          }
        }
      }
    }
}
