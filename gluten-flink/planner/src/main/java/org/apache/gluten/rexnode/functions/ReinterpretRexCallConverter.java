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
import org.apache.gluten.util.LogicalTypeConverter;

import io.github.zhztheplayer.velox4j.expression.CallTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.runtime.types.PlannerTypeUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DATE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_DAY_TIME;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_YEAR_MONTH;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE;

public class ReinterpretRexCallConverter extends BaseRexCallConverter {

  private static final Logger LOG = LoggerFactory.getLogger(ReinterpretRexCallConverter.class);
  private static final String FUNCTION_NAME = "cast";

  public ReinterpretRexCallConverter() {
    super(FUNCTION_NAME);
  }

  private boolean isSupported(LogicalTypeRoot targetType, LogicalTypeRoot resultType) {
    // internal reinterpretation of temporal types: from Flink
    // `ScalaOperatorGens#generateReinterpret`
    // Date -> Integer
    // Time -> Integer
    // Timestamp -> Long
    // Integer -> Date
    // Integer -> Time
    // Long -> Timestamp
    // Integer -> Interval Months
    // Long -> Interval Millis
    // Interval Months -> Integer
    // Interval Millis -> Long
    // Date -> Long
    // Time -> Long
    // Interval Months -> Long
    if (targetType.equals(DATE) && resultType.equals(INTEGER)) {
      return true;
    } else if (targetType.equals(TIME_WITHOUT_TIME_ZONE) && resultType.equals(INTEGER)) {
      return true;
    } else if (targetType.equals(INTEGER) && resultType.equals(DATE)) {
      return true;
    } else if (targetType.equals(INTEGER) && resultType.equals(TIME_WITHOUT_TIME_ZONE)) {
      return true;
    } else if (targetType.equals(INTEGER) && resultType.equals(INTERVAL_YEAR_MONTH)) {
      return true;
    } else if (targetType.equals(BIGINT) && resultType.equals(INTERVAL_DAY_TIME)) {
      return true;
    } else if (targetType.equals(INTERVAL_YEAR_MONTH) && resultType.equals(INTEGER)) {
      return true;
    } else if (targetType.equals(INTERVAL_DAY_TIME) && resultType.equals(BIGINT)) {
      return true;
    } else if (targetType.equals(DATE) && resultType.equals(BIGINT)) {
      return true;
    } else if (targetType.equals(TIME_WITHOUT_TIME_ZONE) && resultType.equals(BIGINT)) {
      return true;
    } else if (targetType.equals(INTERVAL_YEAR_MONTH) && resultType.equals(BIGINT)) {
      return true;
    } else if (targetType.equals(TIMESTAMP_WITHOUT_TIME_ZONE) && resultType.equals(BIGINT)) {
      return true;
    } else if (targetType.equals(BIGINT) && resultType.equals(TIMESTAMP_WITHOUT_TIME_ZONE)) {
      return true;
    }
    return false;
  }

  @Override
  public boolean isSupported(RexCall callNode, RexConversionContext context) {
    if (callNode.getOperands().size() != 1) {
      return false;
    }
    RexNode rexNode = callNode.getOperands().get(0);
    LogicalType resultType = FlinkTypeFactory.toLogicalType(callNode.getType());
    LogicalType targetType = FlinkTypeFactory.toLogicalType(rexNode.getType());
    if (PlannerTypeUtils.isInteroperable(targetType, resultType)) {
      return true;
    } else if (resultType.equals(targetType)) {
      return true;
    } else {
      LogicalTypeRoot targetTypeRoot = targetType.getTypeRoot();
      LogicalTypeRoot resultTypeRoot = resultType.getTypeRoot();
      return isSupported(targetTypeRoot, resultTypeRoot);
    }
  }

  private List<String> getRexNodeInputNames(RexNode node, List<String> fieldNames) {
    final List<String> inputNames = new ArrayList<>();

    Consumer<List<String>> updateInputNames =
        (List<String> otherNames) -> {
          for (int i = 0; i < otherNames.size(); ++i) {
            String otherName = otherNames.get(i);
            if (i < inputNames.size()) {
              if (inputNames.get(i) == null) {
                inputNames.set(i, otherName);
              }
            } else {
              inputNames.add(otherName);
            }
          }
        };

    if (node instanceof RexFieldAccess) {
      RexFieldAccess fieldAccess = (RexFieldAccess) node;
      List<String> refExprInputNames =
          getRexNodeInputNames(fieldAccess.getReferenceExpr(), fieldNames);
      updateInputNames.accept(refExprInputNames);
    } else if (node instanceof RexCall) {
      RexCall rexCall = (RexCall) node;
      List<RexNode> rexNodes = rexCall.getOperands();
      for (RexNode rexNode : rexNodes) {
        List<String> rexNodeInputNames = getRexNodeInputNames(rexNode, fieldNames);
        updateInputNames.accept(rexNodeInputNames);
      }
    } else if (node instanceof RexInputRef) {
      RexInputRef inputRef = (RexInputRef) node;
      String[] inputRefNames = new String[inputRef.getIndex() + 1];
      inputRefNames[inputRef.getIndex()] = fieldNames.get(inputRef.getIndex());
      updateInputNames.accept(Arrays.asList(inputRefNames));
    }
    return inputNames;
  }

  private TypedExpr convertRexCallToTypeExpr(RexCall call, RexConversionContext context) {
    RexConversionContext subContext =
        new RexConversionContext(getRexNodeInputNames(call, context.getInputAttributeNames()));
    RexCallConverter converter = RexCallConverterFactory.getConverter((RexCall) call, subContext);
    return converter.toTypedExpr((RexCall) call, subContext);
  }

  @Override
  public TypedExpr toTypedExpr(RexCall callNode, RexConversionContext context) {
    RexNode operand = callNode.getOperands().get(0);
    LogicalType resultType = FlinkTypeFactory.toLogicalType(operand.getType());
    LogicalType targetType = FlinkTypeFactory.toLogicalType(callNode.getType());
    if (PlannerTypeUtils.isInteroperable(resultType, targetType)) {
      if (operand instanceof RexCall) {
        CallTypedExpr operandExpr =
            (CallTypedExpr) convertRexCallToTypeExpr((RexCall) operand, context);
        return new CallTypedExpr(
            LogicalTypeConverter.toVLType(targetType),
            operandExpr.getInputs(),
            operandExpr.getFunctionName());
      } else {
        throw new RuntimeException("Not implemented for types interoperable");
      }
    } else if (resultType.equals(targetType)) {
      if (operand instanceof RexCall) {
        return convertRexCallToTypeExpr((RexCall) operand, context);
      } else {
        throw new RuntimeException("Not implemented for type equals when operand is not RexCall.");
      }
    } else {
      throw new RuntimeException(
          "Not implemented for types between int/bigint and timestamp/date.");
    }
  }
}
