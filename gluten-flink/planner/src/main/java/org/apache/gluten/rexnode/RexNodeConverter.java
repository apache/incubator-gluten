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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.gluten.substrait.SubstraitContext;
import org.apache.gluten.substrait.expression.ExpressionBuilder;
import org.apache.gluten.substrait.expression.ExpressionNode;
import org.apache.gluten.substrait.type.TypeBuilder;
import org.apache.gluten.substrait.type.TypeNode;

import java.util.List;
import java.util.stream.Collectors;

/** Convertor to convert RexNode to ExpressionNode */
public class RexNodeConverter {

    public static ExpressionNode toExpressionNode(SubstraitContext context, RexNode rexNode) {
        if (rexNode instanceof RexLiteral) {
            RexLiteral literal = (RexLiteral) rexNode;
            return ExpressionBuilder.makeLiteral(
                    literal.getValue(),
                    toTypeNode(literal.getType()));
        } else if (rexNode instanceof RexCall) {
            RexCall rexCall = (RexCall) rexNode;
            Long functionId =
                    ExpressionBuilder.newScalarFunction(
                            context.registeredFunction(),
                            rexCall.getOperator().getName());
            List<ExpressionNode> params = toExpressionNode(context, rexCall.getOperands());
            TypeNode typeNode = toTypeNode(rexCall.getType());
            return ExpressionBuilder.makeScalarFunction(functionId, params, typeNode);
        } else if (rexNode instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) rexNode;
            return ExpressionBuilder.makeSelection(inputRef.getIndex());
        } else {
            throw new RuntimeException("Unrecognized RexNode: " + rexNode);
        }
    }

    public static List<ExpressionNode> toExpressionNode(
            SubstraitContext context,
            List<RexNode> rexNodes) {
        return rexNodes.stream()
                .map(rexNode -> toExpressionNode(context, rexNode))
                .collect(Collectors.toList());
    }

    public static TypeNode toTypeNode(RelDataType dataType) {
        switch (dataType.getSqlTypeName()) {
            case BOOLEAN:
                return TypeBuilder.makeBoolean(dataType.isNullable());
            case TINYINT:
                return TypeBuilder.makeI8(dataType.isNullable());
            case SMALLINT:
                return TypeBuilder.makeI16(dataType.isNullable());
            case INTEGER:
                return TypeBuilder.makeI32(dataType.isNullable());
            case BIGINT:
                return TypeBuilder.makeI64(dataType.isNullable());
            case FLOAT:
                return TypeBuilder.makeFP32(dataType.isNullable());
            case DOUBLE:
                return TypeBuilder.makeFP64(dataType.isNullable());
            case CHAR:
                return TypeBuilder.makeFixedChar(dataType.isNullable(), 1);
            case VARCHAR:
                return TypeBuilder.makeString(dataType.isNullable());
            case BINARY:
                return TypeBuilder.makeBinary(dataType.isNullable());
            case DECIMAL:
                return TypeBuilder.makeDecimal(
                        dataType.isNullable(),
                        dataType.getPrecision(),
                        dataType.getScale());
            case DATE:
                return TypeBuilder.makeDate(dataType.isNullable());
            case TIME:
                return TypeBuilder.makeTimestamp(dataType.isNullable());
            case MAP:
                return TypeBuilder.makeMap(
                        dataType.isNullable(),
                        toTypeNode(dataType.getKeyType()),
                        toTypeNode(dataType.getValueType()));
            default:
                throw new RuntimeException("Unsupported rex node type: " + dataType);
        }
    }

}
