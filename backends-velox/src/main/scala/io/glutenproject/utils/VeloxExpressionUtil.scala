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

import io.glutenproject.expression.ConverterUtils.{CAST, LENGTH, JSON_ARRAY_LENGTH, REGEXP_REPLACE, ROUND, SPLIT, SPLIT_PART}
import org.apache.spark.sql.types.DataTypes

object VeloxExpressionUtil {


  /**
   * The blacklist for Velox unsupported or mismatched expressions with specific input type,
   * such as Cast(ArrayType)
   */
  // The expression with empty type will fall back directly.
  final val EMPTY_TYPE = ""
  final val ARRAY_TYPE = "array"
  final val MAP_TYPE = "map"
  final val STRUCT_TYPE = "struct"
  final val VELOX_EXPR_BLACKLIST: Map[String, Set[String]] = Map(
    CAST -> Set(ARRAY_TYPE, MAP_TYPE, STRUCT_TYPE),
    ROUND -> Set(EMPTY_TYPE),
    REGEXP_REPLACE -> Set(EMPTY_TYPE),
    SPLIT -> Set(EMPTY_TYPE),
    SPLIT_PART -> Set(EMPTY_TYPE),
    LENGTH -> Set(DataTypes.BinaryType.typeName),
    // to be removed when Velox support compatible type
    JSON_ARRAY_LENGTH -> Set(EMPTY_TYPE)
    )
}
