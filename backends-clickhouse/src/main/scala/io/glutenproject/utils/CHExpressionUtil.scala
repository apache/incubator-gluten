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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, GetJsonObject, Literal}

trait FunctionValidator extends Logging {
  def doValidate(expr: Expression): Boolean = false
}

class DefaultBlackList() extends FunctionValidator {
  override def doValidate(expr: Expression): Boolean = false
}

class UnixTimeStampValidator() extends FunctionValidator {
  final val DATE_TYPE = "date"

  override def doValidate(expr: Expression): Boolean = {
    !expr.children.map(_.dataType.typeName).exists(DATE_TYPE.contains)
  }
}

class GetJsonObjectValidator() extends FunctionValidator {
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

class ValidatorUtil(validator: FunctionValidator) {
  def doValidate(expr: Expression): Boolean = validator.doValidate(expr)
}
object CHExpressionUtil {

  /**
   * The blacklist for Clickhouse unsupported or mismatched expression / aggregate function with
   * specific input type.
   */
  final val EMPTY_TYPE = ""
  final val ARRAY_TYPE = "array"
  final val MAP_TYPE = "map"
  final val STRUCT_TYPE = "struct"
  final val STRING_TYPE = "string"

  final val CH_AGGREGATE_FUNC_BLACKLIST: Map[String, ValidatorUtil] = Map(
    STDDEV -> new ValidatorUtil(new DefaultBlackList),
    VAR_SAMP -> new ValidatorUtil(new DefaultBlackList),
    VAR_POP -> new ValidatorUtil(new DefaultBlackList),
    BLOOM_FILTER_AGG -> new ValidatorUtil(new DefaultBlackList),
    CORR -> new ValidatorUtil(new DefaultBlackList),
    FIRST -> new ValidatorUtil(new DefaultBlackList),
    LAST -> new ValidatorUtil(new DefaultBlackList),
    COVAR_POP -> new ValidatorUtil(new DefaultBlackList),
    COVAR_SAMP -> new ValidatorUtil(new DefaultBlackList)
  )

  final val CH_BLACKLIST_SCALAR_FUNCTION: Map[String, ValidatorUtil] = Map(
    SPLIT_PART -> new ValidatorUtil(new DefaultBlackList),
    TO_UNIX_TIMESTAMP -> new ValidatorUtil(new UnixTimeStampValidator),
    UNIX_TIMESTAMP -> new ValidatorUtil(new UnixTimeStampValidator),
    MIGHT_CONTAIN -> new ValidatorUtil(new DefaultBlackList),
    MAKE_DECIMAL -> new ValidatorUtil(new DefaultBlackList),
    UNSCALED_VALUE -> new ValidatorUtil(new DefaultBlackList),
    GET_JSON_OBJECT -> new ValidatorUtil(new GetJsonObjectValidator)
  )
}
