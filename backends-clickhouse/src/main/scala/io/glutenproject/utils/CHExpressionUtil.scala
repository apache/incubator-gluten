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
package io.glutenproject.utils

import io.glutenproject.expression.ExpressionMappings._

object CHExpressionUtil {

  /**
   * The blacklist for Clickhouse unsupported or mismatched expression / aggregate function with
   * specific input type.
   */
  final val EMPTY_TYPE = ""
  final val ARRAY_TYPE = "array"
  final val MAP_TYPE = "map"
  final val STRUCT_TYPE = "struct"
  final val DATE_TYPE = "date"

  final val CH_EXPR_BLACKLIST: Map[String, Set[String]] = Map(
    REGEXP_EXTRACT_ALL -> Set(EMPTY_TYPE),
    REGEXP_EXTRACT -> Set(EMPTY_TYPE),
    LOCATE -> Set(EMPTY_TYPE),
    JSON_ARRAY_LENGTH -> Set(EMPTY_TYPE),
    SHA1 -> Set(EMPTY_TYPE),
    CRC32 -> Set(EMPTY_TYPE),
    SPLIT_PART -> Set(EMPTY_TYPE),
    TO_UNIX_TIMESTAMP -> Set(DATE_TYPE),
    UNIX_TIMESTAMP -> Set(DATE_TYPE),
    // CH arrayJoin function doesn't support map type argument yet.
    // Wait for https://github.com/ClickHouse/ClickHouse/pull/43239
    EXPLODE -> Set(MAP_TYPE),
    MIGHT_CONTAIN -> Set(EMPTY_TYPE)
  )

  final val CH_AGGREGATE_FUNC_BLACKLIST: Map[String, Set[String]] = Map(
    STDDEV_SAMP -> Set(EMPTY_TYPE),
    COLLECT_LIST -> Set(EMPTY_TYPE),
    BLOOM_FILTER_AGG -> Set(EMPTY_TYPE)
  )
}
