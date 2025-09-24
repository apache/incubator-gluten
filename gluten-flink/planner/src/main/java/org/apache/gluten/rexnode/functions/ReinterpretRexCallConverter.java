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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DATE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_DAY_TIME;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTERVAL_YEAR_MONTH;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE;

/**
 * Support function `Reinterpret`, it will be used to handle expression field when it refers to
 * watermark computing. e.g.
 *
 * <pre>{@code
 * create table s(
 *   a int,
 *   c string,
 *   d as case when a = 1 then cast(c as timestamp(3)) else cast(b as timestamp(3)) end,
 *   WATERMARK FOR d AS d - INTERVAL '4' SECOND
 * ) with (
 *   'connector' = 'kafka'
 * );
 *
 * explain select d from s where a = 1;
 *
 * The plan show as below:
 *
 * Calc(select=[Reinterpret(CASE(=(a, 1), CAST(c AS TIMESTAMP(3)), CAST(b AS TIMESTAMP(3)))) AS d], where=[=(a, 1)])
 *   +- TableSourceScan(table=[[default_catalog, default_database, s, watermark=[-(CASE(=(a, 1), CAST(c AS TIMESTAMP(3)), CAST(b AS TIMESTAMP(3))), 4000:INTERVAL SECOND)] ....)
 * }</pre>
 */
public class ReinterpretRexCallConverter extends BaseRexCallConverter {

  private static final String FUNCTION_NAME = "cast";
  /**
   * internal reinterpretation of temporal types: from Flink `ScalaOperatorGens#generateReinterpret`
   *
   * <pre>{@code
   * Date -> Integer
   * Time -> Integer
   * Timestamp -> Long
   * Integer -> Date
   * Integer -> Time
   * Long -> Timestamp
   * Integer -> Interval Months
   * Long -> Interval Millis
   * Interval Months -> Integer
   * Interval Millis -> Long
   * Date -> Long
   * Time -> Long
   * Interval Months -> Long
   * }</pre>
   *
   * TODO: support convert between BIGINT and TIMESTMP_WITH_OUT_TIMEZONE.
   */
  private final Map<LogicalTypeRoot, Set<LogicalTypeRoot>> supportedTypes =
      Map.of(
          DATE, Set.of(INTEGER, BIGINT),
          TIME_WITHOUT_TIME_ZONE, Set.of(INTEGER, BIGINT),
          INTERVAL_YEAR_MONTH, Set.of(INTEGER, BIGINT),
          INTERVAL_DAY_TIME, Set.of(BIGINT),
          INTEGER, Set.of(DATE, TIME_WITHOUT_TIME_ZONE, INTERVAL_YEAR_MONTH),
          BIGINT, Set.of(INTERVAL_DAY_TIME));

  public ReinterpretRexCallConverter() {
    super(FUNCTION_NAME);
  }

  private boolean isSuitable(LogicalTypeRoot targetType, LogicalTypeRoot resultType) {
    if (supportedTypes.containsKey(targetType)) {
      Set<LogicalTypeRoot> valueTypes = supportedTypes.get(targetType);
      return valueTypes.contains(resultType);
    } else {
      return false;
    }
  }

  @Override
  public ValidationResult isSuitable(RexCall callNode, RexConversionContext context) {
    if (callNode.getOperands().size() != 1) {
      return ValidationResult.failure("Function reinterpret operands number must be 1.");
    }
    RexNode rexNode = callNode.getOperands().get(0);
    if (!(rexNode instanceof RexCall)) {
      return ValidationResult.failure("Function reinterpret operand type is not RexCall.");
    }
    LogicalType resultType = FlinkTypeFactory.toLogicalType(callNode.getType());
    LogicalType targetType = FlinkTypeFactory.toLogicalType(rexNode.getType());
    if (PlannerTypeUtils.isInteroperable(targetType, resultType)) {
      return ValidationResult.success();
    } else if (resultType.getTypeRoot() == targetType.getTypeRoot()) {
      return ValidationResult.success();
    } else {
      LogicalTypeRoot targetTypeRoot = targetType.getTypeRoot();
      LogicalTypeRoot resultTypeRoot = resultType.getTypeRoot();
      return isSuitable(targetTypeRoot, resultTypeRoot)
          ? ValidationResult.success()
          : ValidationResult.failure(
              String.format(
                  "Function reinterpret target type %s and result type %s is not supported",
                  targetTypeRoot.name(), resultTypeRoot.name()));
    }
  }

  /**
   * Get the referenced input field names of the RexNode. In order to convert the RexNode call to
   * velox function, we also need to convert the its reference fields to Velox#FieldAccessTypedExpr,
   * and {@link #getRexNodeInputNames} is used to get the referenced field names need to be
   * converted, and the conversion will be made by {@link RexNodeConverter#toTypedExpr}.
   */
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
    RexCall operand = (RexCall) callNode.getOperands().get(0);
    LogicalType resultType = FlinkTypeFactory.toLogicalType(operand.getType());
    LogicalType targetType = FlinkTypeFactory.toLogicalType(callNode.getType());
    if (resultType.getTypeRoot() != targetType.getTypeRoot()) {
      CallTypedExpr operandExpr = (CallTypedExpr) convertRexCallToTypeExpr(operand, context);
      return new CallTypedExpr(
          LogicalTypeConverter.toVLType(targetType),
          operandExpr.getInputs(),
          operandExpr.getFunctionName());
    } else {
      return convertRexCallToTypeExpr(operand, context);
    }
  }
}
