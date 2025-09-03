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
import org.apache.gluten.rexnode.ValidationResult;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.calcite.rex.RexCall;

import java.util.List;
import java.util.stream.Collectors;

class TimestampIntervalRexCallConverter extends BaseRexCallConverter {

  public TimestampIntervalRexCallConverter(String functionName) {
    super(functionName);
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    // TODO: this is not fully completed yet.
    List<Type> operandTypes =
        callNode.getOperands().stream()
            .map(param -> RexNodeConverter.toType(param.getType()))
            .collect(Collectors.toList());
    if (operandTypes.get(0) instanceof TimestampType
        // && TypeUtils.isTimeInterval(operandTypes.get(1)))
        || // (TypeUtils.isTimeInterval(operandTypes.get(0)) &&
        operandTypes.get(1) instanceof TimestampType) {
      return ValidationResult.success();
    } else {
      return ValidationResult.failure("Parameters are not TimestampType");
    }
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    Type resultType = getResultType(callNode);
    // TODO: for comparison, should return boolean. Refine it.
    if (!(resultType instanceof BooleanType)) {
      resultType = new BigIntType();
    }
    return new CallTypedExpr(
        resultType,
        List.of(
            CastTypedExpr.create(new BigIntType(), params.get(0), false),
            CastTypedExpr.create(new BigIntType(), params.get(1), false)),
        functionName);
  }
}
