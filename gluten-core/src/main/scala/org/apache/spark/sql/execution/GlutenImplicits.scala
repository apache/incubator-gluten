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
package org.apache.spark.sql.execution

import org.apache.spark.sql.Dataset

// spotless:off
/**
 * A helper class to get the Gluten fallback summary from a Spark [[Dataset]].
 *
 * Note that, if AQE is enabled, but the query is not materialized, then this method will re-plan
 * the query execution with disabled AQE. It is a workaround to get the final plan, and it may
 * cause the inconsistent results with a materialized query. However, we have no choice.
 *
 * For example:
 *
 * {{{
 *   import org.apache.spark.sql.execution.GlutenImplicits._
 *   val df = spark.sql("SELECT * FROM t")
 *   df.fallbackSummary
 * }}}
 */
// spotless:on
object GlutenImplicits {

  implicit class DatasetTransformer[T](dateset: Dataset[T]) {
    def fallbackSummary(): FallbackSummary = {
      FallbackSummary(dateset.sparkSession, dateset.queryExecution)
    }
  }
}
