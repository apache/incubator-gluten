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
import io.glutenproject.utils.GlutenExpressionUtil.{ARRAY_TYPE, EMPTY_TYPE, MAP_TYPE, STRUCT_TYPE}

import org.apache.spark.sql.types.DataTypes

object VeloxExpressionUtil {

  /**
   * The blacklist for Velox unsupported or mismatched expressions with specific input type, such as
   * Cast(ArrayType)
   */
  // The expression with empty type will fall back directly.
  final val VELOX_EXPR_BLACKLIST: Map[String, Set[String]] = Map(
    CAST -> Set(ARRAY_TYPE, MAP_TYPE, STRUCT_TYPE),
    REGEXP_REPLACE -> Set(EMPTY_TYPE),
    SPLIT -> Set(EMPTY_TYPE),
    SPLIT_PART -> Set(EMPTY_TYPE),
    LENGTH -> Set(DataTypes.BinaryType.typeName),
    FACTORIAL -> Set(EMPTY_TYPE),
    CONCAT_WS -> Set(EMPTY_TYPE),
    RAND -> Set(EMPTY_TYPE),
    // to be removed when Velox support compatible type
    JSON_ARRAY_LENGTH -> Set(EMPTY_TYPE),
    DATE_DIFF -> Set(EMPTY_TYPE),
    FROM_UNIXTIME -> Set(EMPTY_TYPE),
    TO_UNIX_TIMESTAMP -> Set(EMPTY_TYPE),
    UNIX_TIMESTAMP -> Set(EMPTY_TYPE)
  )
}
