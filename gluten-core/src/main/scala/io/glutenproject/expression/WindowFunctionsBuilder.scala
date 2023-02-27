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

import io.glutenproject.expression.ConverterUtils.FunctionConfig
import io.glutenproject.substrait.expression.ExpressionBuilder
import org.apache.spark.sql.catalyst.expressions.{CumeDist, DenseRank, PercentRank, Rank, RowNumber, WindowFunction}

object WindowFunctionsBuilder {
  def create(args: java.lang.Object, windowFunc: WindowFunction): Long = {
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val substraitFunc = ExpressionMappings.window_functions_map.get(windowFunc.getClass)
    if (substraitFunc.isDefined) {
      val functionName = windowFunc match {
        case _: RowNumber | _: Rank | _: DenseRank | _: PercentRank | _: CumeDist =>
          ConverterUtils.makeFuncName(
            substraitFunc.get,
            Seq(windowFunc.dataType),
            FunctionConfig.OPT)
        case other =>
          throw new UnsupportedOperationException(s"not currently supported: $other.")
      }
      ExpressionBuilder.newScalarFunction(functionMap, functionName)
    } else {
      throw new UnsupportedOperationException(
        s"not currently supported: ${windowFunc.getClass.getName}.")
    }
  }
}
