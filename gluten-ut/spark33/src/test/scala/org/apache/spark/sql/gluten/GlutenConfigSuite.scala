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

package org.apache.spark.sql.gluten

import io.glutenproject.GlutenConfig
import org.apache.spark.sql.GlutenSQLTestsTrait
import org.apache.spark.sql.internal.SQLConf

class GlutenConfigSuite extends GlutenSQLTestsTrait {

  test("Disable vectorized reader when gluten enable") {
    withTable("t") {
      spark.range(10).write.format("parquet").saveAsTable("t")

      Seq("true", "false").foreach(enableGluten => {
        Seq("true", "false").foreach(enableVanillaVectorizedReader => {
          withSQLConf(
            GlutenConfig.GLUTEN_ENABLE_KEY -> enableGluten,
            GlutenConfig.VANILLA_VECTORIZED_READERS_ENABLED.key -> enableVanillaVectorizedReader) {
            (enableGluten, enableVanillaVectorizedReader) match {
              case ("true", "true") =>
                sql("SELECT * FROM t").explain()
                assert(spark.conf.get(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key)
                  .equalsIgnoreCase(enableVanillaVectorizedReader))
                assert(spark.conf.get(SQLConf.ORC_VECTORIZED_READER_ENABLED.key)
                  .equalsIgnoreCase(enableVanillaVectorizedReader))
                assert(spark.conf.get(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key)
                  .equalsIgnoreCase(enableVanillaVectorizedReader))
              case ("true", "false") =>
                sql("SELECT * FROM t").explain()
                assert(spark.conf.get(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key)
                  .equalsIgnoreCase(enableVanillaVectorizedReader))
                assert(spark.conf.get(SQLConf.ORC_VECTORIZED_READER_ENABLED.key).
                  equalsIgnoreCase(enableVanillaVectorizedReader))
                assert(spark.conf.get(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key).
                  equalsIgnoreCase(enableVanillaVectorizedReader))
              case ("false", "true") =>
                sql("SELECT * FROM t").explain()
                // Since all rules were controlled by spark.gluten.enabled,
                // we could not recover the configs changed by Gluten.
                assert(!spark.conf.get(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key)
                  .equalsIgnoreCase(enableVanillaVectorizedReader))
                assert(!spark.conf.get(SQLConf.ORC_VECTORIZED_READER_ENABLED.key)
                  .equalsIgnoreCase(enableVanillaVectorizedReader))
                assert(!spark.conf.get(SQLConf.CACHE_VECTORIZED_READER_ENABLED.key)
                  .equalsIgnoreCase(enableVanillaVectorizedReader))
              case _ =>
            }
          }
        })
      })
    }
  }

}
