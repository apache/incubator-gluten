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

import org.apache.gluten.expression.ConverterUtils.FunctionConfig
import org.apache.gluten.extension.ExpressionExtensionTrait
import org.apache.gluten.substrait.expression.ExpressionBuilder

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction

// Static helper object for handling expressions that are specifically used in CH backend.
object CHExpressions {
  // Since https://github.com/apache/incubator-gluten/pull/1937.
  def createAggregateFunction(args: java.lang.Object, aggregateFunc: AggregateFunction): Long = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    if (
      ExpressionExtensionTrait.expressionExtensionTransformer.extensionExpressionsMapping.contains(
        aggregateFunc.getClass)
    ) {
      val (substraitAggFuncName, inputTypes) =
        ExpressionExtensionTrait.expressionExtensionTransformer.buildCustomAggregateFunction(
          aggregateFunc)
      assert(substraitAggFuncName.isDefined)
      return ExpressionBuilder.newScalarFunction(
        functionMap,
        ConverterUtils.makeFuncName(substraitAggFuncName.get, inputTypes, FunctionConfig.REQ))
    }

    AggregateFunctionsBuilder.create(args, aggregateFunc)
  }
}
