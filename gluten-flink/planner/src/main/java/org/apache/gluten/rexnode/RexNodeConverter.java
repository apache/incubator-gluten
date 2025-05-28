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
package org.apache.gluten.rexnode;

import org.apache.gluten.rexnode.functions.RexCallConverter;
import org.apache.gluten.rexnode.functions.RexCallConverterFactory;
import org.apache.gluten.util.LogicalTypeConverter;

import io.github.zhztheplayer.velox4j.expression.ConstantTypedExpr;
import io.github.zhztheplayer.velox4j.expression.FieldAccessTypedExpr;
import io.github.zhztheplayer.velox4j.expression.TypedExpr;
import io.github.zhztheplayer.velox4j.type.Type;
import io.github.zhztheplayer.velox4j.variant.BigIntValue;
import io.github.zhztheplayer.velox4j.variant.BooleanValue;
import io.github.zhztheplayer.velox4j.variant.DoubleValue;
import io.github.zhztheplayer.velox4j.variant.HugeIntValue;
import io.github.zhztheplayer.velox4j.variant.IntegerValue;
import io.github.zhztheplayer.velox4j.variant.SmallIntValue;
import io.github.zhztheplayer.velox4j.variant.TinyIntValue;
import io.github.zhztheplayer.velox4j.variant.VarBinaryValue;
import io.github.zhztheplayer.velox4j.variant.VarCharValue;
import io.github.zhztheplayer.velox4j.variant.Variant;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

/** Convertor to convert RexNode to velox TypedExpr */
public class RexNodeConverter {

  public static TypedExpr toTypedExpr(RexNode rexNode, RexConversionContext context) {
    if (rexNode instanceof RexLiteral) {
      RexLiteral literal = (RexLiteral) rexNode;
      return new ConstantTypedExpr(toType(literal.getType()), toVariant(literal), null);
    } else if (rexNode instanceof RexCall) {
      RexCall rexCall = (RexCall) rexNode;
      String operatorName = rexCall.getOperator().getName();
      RexCallConverter converter = RexCallConverterFactory.getConverter(operatorName);
      return converter.toTypedExpr(rexCall, context);
    } else if (rexNode instanceof RexInputRef) {
      RexInputRef inputRef = (RexInputRef) rexNode;
      List<String> inputAttributes = context.getInputAttributeNames();
      return FieldAccessTypedExpr.create(
          toType(inputRef.getType()), inputAttributes.get(inputRef.getIndex()));
    } else if (rexNode instanceof RexFieldAccess) {
      RexFieldAccess fieldAccess = (RexFieldAccess) rexNode;
      return FieldAccessTypedExpr.create(
          toTypedExpr(fieldAccess.getReferenceExpr(), context), fieldAccess.getField().getName());
    } else {
      throw new RuntimeException("Unrecognized RexNode: " + rexNode.getClass().getName());
    }
  }

  public static List<TypedExpr> toTypedExpr(List<RexNode> rexNodes, RexConversionContext context) {
    return rexNodes.stream()
        .map(rexNode -> toTypedExpr(rexNode, context))
        .collect(Collectors.toList());
  }

  // TODO: use LogicalRelDataTypeConverter
  public static Type toType(RelDataType relDataType) {
    return LogicalTypeConverter.toVLType(FlinkTypeFactory.toLogicalType(relDataType));
  }

  public static Variant toVariant(RexLiteral literal) {
    switch (literal.getType().getSqlTypeName()) {
      case BOOLEAN:
        return new BooleanValue((boolean) literal.getValue());
      case TINYINT:
        return new TinyIntValue(Integer.valueOf(literal.getValue().toString()));
      case SMALLINT:
        return new SmallIntValue(Integer.valueOf(literal.getValue().toString()));
      case INTEGER:
        return new IntegerValue(Integer.valueOf(literal.getValue().toString()));
      case BIGINT:
        return new BigIntValue(Long.valueOf(literal.getValue().toString()));
      case DOUBLE:
        return new DoubleValue(Double.valueOf(literal.getValue().toString()));
      case VARCHAR:
        return new VarCharValue(literal.getValue().toString());
      case BINARY:
        return VarBinaryValue.create(literal.getValue().toString().getBytes(StandardCharsets.UTF_8));
      case DECIMAL:
      case INTERVAL_SECOND:
        // interval is used as decimal.
        // TODO: fix precision check
        BigDecimal bigDecimal = literal.getValueAs(BigDecimal.class);
        if (bigDecimal.precision() <= 18) {
          return new BigIntValue(bigDecimal.unscaledValue().longValueExact());
        } else {
          return new HugeIntValue(bigDecimal.unscaledValue());
        }
      default:
        throw new RuntimeException(
            "Unsupported rex node type: " + literal.getType().getSqlTypeName());
    }
  }
}
