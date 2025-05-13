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
package org.apache.gluten.rexnode.functions;

import org.apache.flink.util.Preconditions;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.Type;

import java.util.List;

/** Subtract function converter. */
public class SubtractFunctionConverter implements FunctionConverter {
  private final String function;

  public SubtractFunctionConverter(String function) {
    this.function = function;
  }

  @Override
  public CallTypedExpr toVeloxFunction(Type nodeType, List<TypedExpr> params) {
    Preconditions.checkNotNull(params.size() == 2, "Subtract must contain exactly two parameters");

    // TODO: need refine for more type cast
    if (params.get(0).getReturnType().getClass() == TimestampType.class
        && params.get(1).getReturnType().getClass() == BigIntType.class) {
      // hardcode here for next mark watermark whose param1 is Timestamp and 2 is BigInt.
      Type newType = new BigIntType();
      TypedExpr param0 = new CallTypedExpr(newType, List.of(params.get(0)), "cast");
      return new CallTypedExpr(newType, List.of(param0, params.get(1)), function);
    }
    return new CallTypedExpr(nodeType, params, function);
  }
}
