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
import org.apache.gluten.util.ReflectUtils;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.variant.ArrayValue;
import io.github.zhztheplayer.velox4j.variant.Variant;

import org.apache.flink.calcite.shaded.com.google.common.collect.BoundType;
import org.apache.flink.calcite.shaded.com.google.common.collect.Range;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sarg;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class SearchRexCallConverter extends BaseRexCallConverter {

  private static final String FUNCTION_NAME = "in";

  public SearchRexCallConverter() {
    super(FUNCTION_NAME);
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    if (callNode.getOperands().size() != 2) {
      return ValidationResult.failure("Function in operands number must be 2.");
    }
    return ValidationResult.success();
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    RexNode firstOp = callNode.getOperands().get(0);
    RexNode secondOp = callNode.getOperands().get(1);
    TypedExpr firstExpr = RexNodeConverter.toTypedExpr(firstOp, context);
    TypedExpr secondExpr = null;
    if (secondOp instanceof RexLiteral
        && ((RexLiteral) secondOp).getTypeName().equals(SqlTypeName.SARG)) {
      RexLiteral literal = (RexLiteral) secondOp;
      Sarg literalValue = (Sarg) literal.getValue();
      if (!literalValue.isPoints()) {
        throw new FlinkRuntimeException("Current only support points value for SARG Type.");
      }
      List<Variant> pointValues = new ArrayList<>();
      Set pointSet = literalValue.rangeSet.asRanges();
      for (Object range : pointSet) {
        if (range instanceof Range) {
          Range r = (Range) range;
          if (r.hasLowerBound()
              && r.hasUpperBound()
              && r.lowerEndpoint().equals(r.upperEndpoint())
              && r.lowerBoundType() == BoundType.CLOSED
              && r.upperBoundType() == BoundType.CLOSED) {
            Comparable<?> point = r.lowerEndpoint();
            RexLiteral subLiteral =
                (RexLiteral)
                    ReflectUtils.invokeObjectMethod(
                        RexLiteral.class,
                        null,
                        "toLiteral",
                        new Class<?>[] {RelDataType.class, Comparable.class},
                        new Object[] {literal.getType(), point});
            pointValues.add(RexNodeConverter.toVariant(subLiteral));
          }
        }
      }
      Variant value = new ArrayValue(pointValues);
      secondExpr = ConstantTypedExpr.create(value);
    } else {
      secondExpr = RexNodeConverter.toTypedExpr(secondOp, context);
    }
    Type resultType = getResultType(callNode);
    return new CallTypedExpr(resultType, List.of(firstExpr, secondExpr), functionName);
  }
}
