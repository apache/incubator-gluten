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

package io.glutenproject.backendsapi.velox
import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.glutendata.GlutenValidatorApi
import io.glutenproject.utils.VeloxExpressionUtil
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction

class VeloxValidatorApi extends GlutenValidatorApi {

  override def doExprValidate(substraitExprName: String, expr: Expression): Boolean = {
    doExprValidate(VeloxExpressionUtil.VELOX_EXPR_BLACKLIST, substraitExprName, expr)
  }

  override def doAggregateFunctionValidate(substraitFuncName: String,
    func: AggregateFunction): Boolean = doAggregateFunctionValidate(
    VeloxExpressionUtil.VELOX_AGGREGATE_FUNC_BLACKLIST, substraitFuncName, func)
}
