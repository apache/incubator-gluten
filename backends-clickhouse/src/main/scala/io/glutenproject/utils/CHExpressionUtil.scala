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

import io.glutenproject.expression.ExpressionNames._

import org.apache.spark.sql.catalyst.expressions.{Expression, GetJsonObject, Literal}

trait FunctionValidator {
  def doValidate(expr: Expression): Boolean
}

case class DefaultValidator() extends FunctionValidator {
  override def doValidate(expr: Expression): Boolean = false
}

case class UnixTimeStampValidator() extends FunctionValidator {
  final val DATE_TYPE = "date"

  override def doValidate(expr: Expression): Boolean = {
    !expr.children.map(_.dataType.typeName).exists(DATE_TYPE.contains)
  }
}

case class GetJsonObjectValidator() extends FunctionValidator {
  override def doValidate(expr: Expression): Boolean = {
    val path = expr.asInstanceOf[GetJsonObject].path
    if (!path.isInstanceOf[Literal]) {
      return false
    }
    val pathStr = path.asInstanceOf[Literal].toString()
    // Not supported: double dot and filter expression
    if (pathStr.contains("..") || pathStr.contains("?(")) {
      return false
    }
    true
  }
}

object CHExpressionUtil {

  final val CH_AGGREGATE_FUNC_BLACKLIST: Map[String, FunctionValidator] = Map(
    BLOOM_FILTER_AGG -> DefaultValidator()
  )

  final val CH_BLACKLIST_SCALAR_FUNCTION: Map[String, FunctionValidator] = Map(
    SPLIT_PART -> DefaultValidator(),
    TO_UNIX_TIMESTAMP -> UnixTimeStampValidator(),
    UNIX_TIMESTAMP -> UnixTimeStampValidator(),
    MIGHT_CONTAIN -> DefaultValidator(),
    GET_JSON_OBJECT -> GetJsonObjectValidator(),
    ARRAYS_OVERLAP -> DefaultValidator(),
    SORT_ARRAY -> DefaultValidator(),
    SKEWNESS -> DefaultValidator()
  )
}
