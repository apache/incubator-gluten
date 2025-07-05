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
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.DecimalType;
import io.github.zhztheplayer.velox4j.type.TimestampType;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.calcite.rex.RexCall;

import java.util.List;

abstract class BaseRexCallConverter implements RexCallConverter {
  protected final String functionName;

  public BaseRexCallConverter(String functionName) {
    this.functionName = functionName;
  }

  protected List<TypedExpr> getParams(RexCall callNode, RexConversionContext context) {
    return RexNodeConverter.toTypedExpr(callNode.getOperands(), context);
  }

  protected Type getResultType(RexCall callNode) {
    return RexNodeConverter.toType(callNode.getType());
  }

  @Override
  public boolean isSupported(RexCall callNode, RexConversionContext context) {
    // Default implementation assumes all RexCall nodes are supported.
    // Subclasses can override this method to provide specific support checks.
    return true;
  }
}

class DefaultRexCallConverter extends BaseRexCallConverter {
  public DefaultRexCallConverter(String functionName) {
    super(functionName);
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    Type resultType = getResultType(callNode);

    if ("cast".equals(functionName) && params.size() == 1) {
      TypedExpr sourceExpr = params.get(0);
      Type sourceType = sourceExpr.getReturnType();

      if (sourceType instanceof TimestampType && resultType instanceof TimestampType) {
        return sourceExpr;
      }
    }

    return new CallTypedExpr(resultType, params, functionName);
  }
}

class BasicArithmeticOperatorRexCallConverter extends BaseRexCallConverter {
  public BasicArithmeticOperatorRexCallConverter(String functionName) {
    super(functionName);
  }

  @Override
  public boolean isSupported(RexCall callNode, RexConversionContext context) {
    return callNode.getOperands().stream()
        .allMatch(param -> TypeUtils.isNumericType(RexNodeConverter.toType(param.getType())));
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);

    Type resultType = getResultType(callNode);
    if (params.size() == 2) {
      Type leftType = params.get(0).getReturnType();
      Type rightType = params.get(1).getReturnType();

      if (leftType instanceof DecimalType || rightType instanceof DecimalType) {
        List<TypedExpr> alignedParams = TypeUtils.promoteTypeForArithmeticExpressions(params);

        Type alignedLeftType = alignedParams.get(0).getReturnType();
        Type alignedRightType = alignedParams.get(1).getReturnType();

        if (alignedLeftType instanceof DecimalType && alignedRightType instanceof DecimalType) {
          Type veloxResultType =
              calculateDecimalResultType(
                  (DecimalType) alignedLeftType, (DecimalType) alignedRightType, functionName);
          TypedExpr veloxExpr = new CallTypedExpr(veloxResultType, alignedParams, functionName);

          return new CallTypedExpr(resultType, List.of(veloxExpr), "cast");
        }
      }
    }

    List<TypedExpr> alignedParams = TypeUtils.promoteTypeForArithmeticExpressions(params);
    return new CallTypedExpr(resultType, alignedParams, functionName);
  }

  private Type calculateDecimalResultType(
      DecimalType leftType, DecimalType rightType, String operation) {
    int leftPrecision = leftType.getPrecision();
    int leftScale = leftType.getScale();
    int rightPrecision = rightType.getPrecision();
    int rightScale = rightType.getScale();

    int resultPrecision;
    int resultScale;

    switch (operation.toLowerCase()) {
      case "plus":
      case "add":
      case "minus":
      case "subtract":
        // + -：precision = max(p1-s1, p2-s2) + max(s1, s2) + 1
        //        scale = max(s1, s2)
        resultScale = Math.max(leftScale, rightScale);
        resultPrecision =
            Math.max(leftPrecision - leftScale, rightPrecision - rightScale) + resultScale + 1;
        break;

      case "multiply":
        // * ：precision = p1 + p2 + 1, scale = s1 + s2
        resultPrecision = leftPrecision + rightPrecision + 1;
        resultScale = leftScale + rightScale;
        break;

      case "divide":
        // / ：precision = p1 - s1 + s2 + max(6, s1 + p2 + 1)
        //      scale = max(6, s1 + p2 + 1)
        resultScale = Math.max(6, leftScale + rightPrecision + 1);
        resultPrecision = leftPrecision - leftScale + rightScale + resultScale;
        break;

      default:
        resultPrecision = Math.max(leftPrecision, rightPrecision);
        resultScale = Math.max(leftScale, rightScale);
        break;
    }

    if (resultPrecision > 38) {
      resultPrecision = 38;
      resultScale = Math.min(resultScale, resultPrecision);
      if ("divide".equals(operation.toLowerCase())) {
        resultScale = Math.max(Math.min(resultScale, 6), resultPrecision - 32);
      }
    }

    resultScale = Math.min(resultScale, resultPrecision);

    return new DecimalType(resultPrecision, resultScale);
  }
}

class SubtractRexCallConverter extends BaseRexCallConverter {

  public SubtractRexCallConverter() {
    super("subtract");
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);

    if (params.get(0).getReturnType() instanceof TimestampType
        && params.get(1).getReturnType() instanceof BigIntType) {

      Type bigIntType = new BigIntType();
      TypedExpr castExpr = new CallTypedExpr(bigIntType, List.of(params.get(0)), "cast");

      List<TypedExpr> newParams = List.of(castExpr, params.get(1));
      return new CallTypedExpr(bigIntType, newParams, functionName);
    }

    List<TypedExpr> alignedParams = TypeUtils.promoteTypeForArithmeticExpressions(params);
    Type resultType = getResultType(callNode);
    return new CallTypedExpr(resultType, alignedParams, functionName);
  }
}
