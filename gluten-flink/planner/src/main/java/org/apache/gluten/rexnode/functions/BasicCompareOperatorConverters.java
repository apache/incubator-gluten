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
import org.apache.gluten.rexnode.ValidationResult;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.CastTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.calcite.rex.RexCall;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

class StringCompareRexCallConverter extends BaseRexCallConverter {

  public StringCompareRexCallConverter(String functionName) {
    super(functionName);
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    // This converter supports string comparison functions.
    boolean typesValidate =
        callNode.getOperands().stream()
            .allMatch(param -> TypeUtils.isStringType(RexNodeConverter.toType(param.getType())));
    if (!typesValidate) {
      String message =
          String.format(
              "String comparison operation requires all operands to be string types, but found: %s",
              getFunctionProtoTypeName(callNode));
      return ValidationResult.failure(message);
    }
    return ValidationResult.success();
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    Type resultType = getResultType(callNode);
    return new CallTypedExpr(resultType, params, functionName);
  }
}

class StringNumberCompareRexCallConverter extends BaseRexCallConverter {

  public StringNumberCompareRexCallConverter(String functionName) {
    super(functionName);
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    // This converter supports string and numeric comparison functions.
    List<Type> paramTypes =
        callNode.getOperands().stream()
            .map(param -> RexNodeConverter.toType(param.getType()))
            .collect(Collectors.toList());
    boolean typesValidate =
        (TypeUtils.isNumericType(paramTypes.get(0)) && TypeUtils.isStringType(paramTypes.get(1)))
            || (TypeUtils.isStringType(paramTypes.get(0))
                && TypeUtils.isNumericType(paramTypes.get(1)));
    if (!typesValidate) {
      String message =
          String.format(
              "String and numeric comparison operation requires one string and one numeric operand, but found: %s",
              getFunctionProtoTypeName(callNode));
      return ValidationResult.failure(message);
    }
    return ValidationResult.success();
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    Type resultType = getResultType(callNode);
    TypedExpr leftExpr =
        TypeUtils.isNumericType(params.get(0).getReturnType())
            ? params.get(0)
            : CastTypedExpr.create(params.get(1).getReturnType(), params.get(0), false);
    TypedExpr rightExpr =
        TypeUtils.isNumericType(params.get(1).getReturnType())
            ? params.get(1)
            : CastTypedExpr.create(params.get(0).getReturnType(), params.get(1), false);
    return new CallTypedExpr(resultType, Arrays.asList(leftExpr, rightExpr), functionName);
  }
}
