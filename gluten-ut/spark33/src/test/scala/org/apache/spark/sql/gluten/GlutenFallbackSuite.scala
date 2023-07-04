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

class GlutenFallbackSuite extends GlutenSQLTestsTrait {

  test("test fallback logging") {
    val testAppender = new LogAppender("fallback reason")
    withLogAppender(testAppender) {
      withSQLConf(
        GlutenConfig.COLUMNAR_FILESCAN_ENABLED.key -> "true",
        GlutenConfig.VALIDATE_FAILURE_LOG_LEVEL.key -> "info") {
        withTable("t") {
          spark.range(10).write.format("parquet").saveAsTable("t")
          sql("SELECT * FROM t").collect()
        }
      }
      assert(testAppender.loggingEvents.exists(_.getMessage.getFormattedMessage.contains(
        "Validation failed for plan: Scan parquet default.t, " +
          "due to: columnar FileScan is not enabled in FileSourceScanExec")))
    }
  }
}
