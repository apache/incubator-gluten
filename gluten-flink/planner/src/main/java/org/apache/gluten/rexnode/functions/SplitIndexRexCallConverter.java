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

import org.apache.gluten.rexnode.RexConversionContext;
import org.apache.gluten.rexnode.RexNodeConverter;
import org.apache.gluten.rexnode.TypeUtils;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.type.VarCharType;
import io.github.zhztheplayer.velox4j.variant.BigIntValue;
import io.github.zhztheplayer.velox4j.variant.IntegerValue;
import io.github.zhztheplayer.velox4j.variant.VarCharValue;

import org.apache.calcite.rex.RexCall;

import java.util.List;

public class SplitIndexRexCallConverter extends BaseRexCallConverter {

  private static final String FUNCTION_NAME = "split_index";

  public SplitIndexRexCallConverter() {
    super(FUNCTION_NAME);
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    ConstantTypedExpr delimiterExpr = (ConstantTypedExpr) params.get(1);
    ConstantTypedExpr indexExpr = (ConstantTypedExpr) params.get(2);
    // The delimiter may be input by its ascii, so here we need to judge it and convert it
    // from ascii to string.
    if (TypeUtils.isIntegerType(delimiterExpr.getReturnType())) {
      IntegerValue intValue = (IntegerValue) delimiterExpr.getValue();
      int val = intValue.getValue();
      VarCharValue varcharValue = new VarCharValue(Character.toString((char) val));
      delimiterExpr = new ConstantTypedExpr(new VarCharType(), varcharValue, null);
      params.set(1, delimiterExpr);
    }
    // The `Index` parameter start from 0, and it is `Int` type in flink; while it start from 1,
    // and it is `BigInt(int64_t)` type in velox. So we need convert the `Index` parameter here.
    if (TypeUtils.isIntegerType(indexExpr.getReturnType())) {
      IntegerValue intValue = (IntegerValue) indexExpr.getValue();
      BigIntValue bigintValue = new BigIntValue(intValue.getValue() + 1);
      indexExpr = new ConstantTypedExpr(new BigIntType(), bigintValue, null);
      params.set(2, indexExpr);
    }
    Type resultType = RexNodeConverter.toType(callNode.getType());
    return new CallTypedExpr(resultType, params, functionName);
  }
}
