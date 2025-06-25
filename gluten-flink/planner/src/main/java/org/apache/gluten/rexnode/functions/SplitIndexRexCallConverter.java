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
import io.github.zhztheplayer.velox4j.variant.BigIntValue;
import io.github.zhztheplayer.velox4j.variant.IntegerValue;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SplitIndexRexCallConverter extends BaseRexCallConverter {

  private static final Logger LOG = LoggerFactory.getLogger(SplitIndexRexCallConverter.class);

  private static final String FUNCTION_NAME = "split_part";

  public SplitIndexRexCallConverter() {
    super(FUNCTION_NAME);
  }

  @Override
  public boolean isSupported(RexCall callNode, RexConversionContext context) {
    if (!TypeUtils.isStringType(RexNodeConverter.toType(callNode.getType()))) {
      return false;
    }
    List<RexNode> operands = callNode.getOperands();
    if (operands.size() != 3) {
      return false;
    }
    RexNode firstOp = operands.get(0);
    RexNode secondOp = operands.get(1);
    RexNode thirdOp = operands.get(2);
    if (!TypeUtils.isStringType(RexNodeConverter.toType(firstOp.getType()))
        || !TypeUtils.isStringType(RexNodeConverter.toType(secondOp.getType()))
        || !TypeUtils.isIntegerType(RexNodeConverter.toType(thirdOp.getType()))) {
      return false;
    }
    if (!(secondOp instanceof RexLiteral) || !(thirdOp instanceof RexLiteral)) {
      return false;
    }
    return true;
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    ConstantTypedExpr indexExpr = (ConstantTypedExpr) params.get(2);
    if (TypeUtils.isIntegerType(indexExpr.getReturnType())) {
      IntegerValue intValue = (IntegerValue) indexExpr.getValue();
      BigIntValue bigintValue = new BigIntValue(intValue.getValue());
      indexExpr = new ConstantTypedExpr(new BigIntType(), bigintValue, null);
      params = List.of(params.get(0), params.get(1), indexExpr);
    }
    Type resultType = RexNodeConverter.toType(callNode.getType());
    return new CallTypedExpr(resultType, params, functionName);
  }
}
