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
import org.apache.gluten.rexnode.TypeUtils;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.Type;

import org.apache.calcite.rex.RexCall;

import java.util.List;

public class ModRexCallConverter extends BasicArithmeticOperatorRexCallConverter {
  private static final String FUNCTION_NAME = "remainder";

  public ModRexCallConverter() {
    super(FUNCTION_NAME);
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    List<TypedExpr> params = getParams(callNode, context);
    List<TypedExpr> alignedParams =
        TypeUtils.promoteTypeForArithmeticExpressions(params.get(0), params.get(1));
    Type resultType = alignedParams.get(0).getReturnType();
    return new CallTypedExpr(resultType, alignedParams, functionName);
  }
}
