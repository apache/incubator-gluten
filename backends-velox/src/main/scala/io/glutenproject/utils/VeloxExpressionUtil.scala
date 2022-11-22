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

import io.glutenproject.expression.ConverterUtils.{CAST, LENGTH, REGEXP_EXTRACT_ALL, REGEXP_REPLACE, ROUND, SPLIT, SPLIT_PART, TRANSLATE, TRIM}
import org.apache.spark.sql.types.DataTypes

object VeloxExpressionUtil {


  /**
   * The blacklist for Velox unsupported or mismatched expressions with specific input type,
   * such as Cast(ArrayType)
   */
  // The expression with empty type will fall back directly.
  final val EMPTY_TYPE = ""
  final val ARRAY_TYPE = "array"
  final val VELOX_EXPR_BLACKLIST: Map[String, String] = Map(
    CAST -> ARRAY_TYPE,
    ROUND -> EMPTY_TYPE,
    REGEXP_REPLACE -> EMPTY_TYPE,
    REGEXP_EXTRACT_ALL -> EMPTY_TYPE,
    SPLIT -> EMPTY_TYPE,
    SPLIT_PART -> EMPTY_TYPE,
    LENGTH -> DataTypes.BinaryType.typeName,
    TRIM -> EMPTY_TYPE,
    TRANSLATE -> EMPTY_TYPE)
}
