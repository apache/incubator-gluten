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
package org.apache.gluten.expression

import org.apache.spark.sql.internal.SQLConf

trait ExpressionRestrictions {
  val functionName: String
  val restrictionMessages: Array[String]
}

object FromJsonRestrictions extends ExpressionRestrictions {
  val FROM_JSON_PARTIAL_RESULTS: String =
    s"${ExpressionNames.FROM_JSON} with 'spark.sql.json.enablePartialResults = false' " +
      s"is not supported in Velox"
  val FROM_JSON_WITH_OPTIONS: String =
    s"${ExpressionNames.FROM_JSON} with options is not supported in Velox"
  val FROM_JSON_CASE_SENSITIVE: String =
    s"${ExpressionNames.FROM_JSON} with " +
      s"'${SQLConf.CASE_SENSITIVE.key} = true' is not supported in Velox"
  val FROM_JSON_WITH_DUPLICATE_KEYS: String =
    s"${ExpressionNames.FROM_JSON} with duplicate keys is not supported in Velox"
  val FROM_JSON_WITH_COLUMN_CORRUPT_RECORD: String =
    s"${ExpressionNames.FROM_JSON} with column corrupt record is not supported in Velox"

  override val functionName: String = ExpressionNames.FROM_JSON

  override val restrictionMessages: Array[String] = Array(
    FROM_JSON_PARTIAL_RESULTS,
    FROM_JSON_WITH_OPTIONS,
    FROM_JSON_CASE_SENSITIVE,
    FROM_JSON_WITH_DUPLICATE_KEYS,
    FROM_JSON_WITH_COLUMN_CORRUPT_RECORD
  )
}

object Unbase64Restrictions extends ExpressionRestrictions {
  val UNBASE64_FAIL_ON_ERROR: String =
    s"${ExpressionNames.UNBASE64} with failOnError is not supported"

  override val functionName: String = ExpressionNames.UNBASE64

  override val restrictionMessages: Array[String] = Array(UNBASE64_FAIL_ON_ERROR)
}

object Base64Restrictions extends ExpressionRestrictions {
  val BASE64_DISABLE_CHUNK_BASE64_STRING: String =
    s"${ExpressionNames.BASE64} with chunkBase64String disabled is not supported"

  override val functionName: String = ExpressionNames.BASE64

  override val restrictionMessages: Array[String] = Array(BASE64_DISABLE_CHUNK_BASE64_STRING)
}

object ExpressionRestrictions {
  def listAllRestrictions(): Array[ExpressionRestrictions] = {
    Array(
      FromJsonRestrictions,
      Unbase64Restrictions,
      Base64Restrictions
    )
  }
}
