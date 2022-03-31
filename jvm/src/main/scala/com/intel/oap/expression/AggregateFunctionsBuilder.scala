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

package com.intel.oap.expression

import com.intel.oap.expression.ConverterUtils.FunctionConfig
import com.intel.oap.substrait.expression.ExpressionBuilder
import org.apache.spark.sql.catalyst.expressions.aggregate._

object AggregateFunctionsBuilder {

  def create(args: java.lang.Object, aggregateFunc: AggregateFunction): Long = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionName = aggregateFunc match {
      case sum: Sum =>
        ConverterUtils.makeFuncName(
          ConverterUtils.SUM, Seq(sum.child.dataType), FunctionConfig.OPT)
      case avg: Average =>
        ConverterUtils.makeFuncName(
          ConverterUtils.AVG, Seq(avg.child.dataType), FunctionConfig.OPT)
      case count: Count =>
        val childrenTypes = count.children.map(child => child.dataType)
        ConverterUtils.makeFuncName(
          ConverterUtils.COUNT, childrenTypes, FunctionConfig.OPT)
      case other =>
        throw new UnsupportedOperationException(s"not currently supported: $other.")
    }
    ExpressionBuilder.newScalarFunction(functionMap, functionName)
  }
}
