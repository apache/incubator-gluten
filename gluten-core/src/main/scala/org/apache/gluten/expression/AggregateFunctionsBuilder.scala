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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.substrait.expression.ExpressionBuilder

import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.DataType

object AggregateFunctionsBuilder {
  def create(args: java.lang.Object, aggregateFunc: AggregateFunction): Long = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

    // First handle the custom aggregate functions
    val (substraitAggFuncName, inputTypes) =
      if (
        ExpressionMappings.expressionExtensionTransformer.extensionExpressionsMapping.contains(
          aggregateFunc.getClass)
      ) {
        val (substraitAggFuncName, inputTypes) =
          ExpressionMappings.expressionExtensionTransformer.buildCustomAggregateFunction(
            aggregateFunc)
        assert(substraitAggFuncName.isDefined)
        (substraitAggFuncName.get, inputTypes)
      } else {
        val substraitAggFuncName = getSubstraitFunctionName(aggregateFunc)

        // Check whether each backend supports this aggregate function.
        if (
          !BackendsApiManager.getValidatorApiInstance.doExprValidate(
            substraitAggFuncName,
            aggregateFunc)
        ) {
          throw new GlutenNotSupportException(
            s"Aggregate function not supported for $aggregateFunc.")
        }

        val inputTypes: Seq[DataType] = aggregateFunc.children.map(child => child.dataType)
        (substraitAggFuncName, inputTypes)
      }

    ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(substraitAggFuncName, inputTypes, FunctionConfig.REQ))
  }

  def getSubstraitFunctionName(aggregateFunc: AggregateFunction): String = {
    aggregateFunc match {
      case First(_, ignoreNulls) if ignoreNulls =>
        ExpressionNames.FIRST_IGNORE_NULL
      case Last(_, ignoreNulls) if ignoreNulls =>
        ExpressionNames.LAST_IGNORE_NULL
      case _ =>
        val nameOpt = ExpressionMappings.expressionsMap.get(aggregateFunc.getClass)
        if (nameOpt.isEmpty) {
          throw new UnsupportedOperationException(
            s"Could not find a valid substrait mapping name for $aggregateFunc.")
        }
        nameOpt.get match {
          case ExpressionNames.UDAF_PLACEHOLDER => aggregateFunc.prettyName
          case name => name
        }
    }
  }
}
