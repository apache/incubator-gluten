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

package io.glutenproject.expression

import io.glutenproject.backendsapi.BackendsApiManager
import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.expression.ExpressionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.DataType

object AggregateFunctionsBuilder {
  def create(args: java.lang.Object, aggregateFunc: AggregateFunction): Long = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

    val substraitAggFuncName = ExpressionMappings.aggregate_functions_map.getOrElse(
      aggregateFunc.getClass, ExpressionMappings.getAggSigOther(aggregateFunc.prettyName))
    // Check whether Gluten supports this aggregate function.
    if (substraitAggFuncName.isEmpty) {
      throw new UnsupportedOperationException(s"not currently supported: $aggregateFunc.")
    }
    // Check whether each backend supports this aggregate function.
    if (!BackendsApiManager.getValidatorApiInstance.doAggregateFunctionValidate(
      substraitAggFuncName, aggregateFunc)) {
      throw new UnsupportedOperationException(s"not currently supported: $aggregateFunc.")
    }

    val inputTypes: Seq[DataType] = aggregateFunc.children.map(child => child.dataType)

    ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitAggFuncName,
        inputTypes,
        FunctionConfig.REQ))
  }
}
