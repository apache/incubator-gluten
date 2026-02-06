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
import org.apache.gluten.rexnode.ValidationResult;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.BooleanType;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.variant.BooleanValue;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class IsTrueRexCallConverter extends BaseRexCallConverter {
  private static final String FUNCTION_NAME = "equalto";

  public IsTrueRexCallConverter() {
    super(FUNCTION_NAME);
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    if (callNode.getOperands().size() != 1) {
      return ValidationResult.failure("Function isTrue operands number must be 1.");
    }
    RexNode rexNode = callNode.getOperands().get(0);
    LogicalType targetType = FlinkTypeFactory.toLogicalType(rexNode.getType());
    if (targetType.is(LogicalTypeRoot.BOOLEAN)) {
      return ValidationResult.success();
    } else {
      return ValidationResult.failure("Parameter is not Boolean type.");
    }
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    TypedExpr constantTrue = new ConstantTypedExpr(new BooleanType(), new BooleanValue(true), null);
    params.add(constantTrue);
    Type resultType = getResultType(callNode);
    return new CallTypedExpr(resultType, params, functionName);
  }
}
