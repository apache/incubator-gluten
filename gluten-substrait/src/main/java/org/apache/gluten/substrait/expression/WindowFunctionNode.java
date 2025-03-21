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
package org.apache.gluten.substrait.expression;

import org.apache.gluten.exception.GlutenException;
import org.apache.gluten.substrait.type.TypeNode;

import io.substrait.proto.Expression;
import io.substrait.proto.FunctionArgument;
import io.substrait.proto.FunctionOption;
import io.substrait.proto.WindowType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class WindowFunctionNode implements Serializable {
  private final Integer functionId;
  private final List<ExpressionNode> expressionNodes = new ArrayList<>();

  private final String columnName;
  private final TypeNode outputTypeNode;

  private final String upperBound;

  private final String lowerBound;

  private final String frameType;

  private final boolean ignoreNulls;

  private final boolean upperBoundFoldable;

  private final boolean lowerBoundFoldable;

  private final long upperBoundOffset;

  private final long lowerBoundOffset;

  private final ExpressionNode upperBoundRefNode;

  private final ExpressionNode lowerBoundRefNode;

  private final boolean isPreComputeRangeFrameUpperBound;

  private final boolean isPreComputeRangeFrameLowerBound;

  WindowFunctionNode(
      Integer functionId,
      List<ExpressionNode> expressionNodes,
      String columnName,
      TypeNode outputTypeNode,
      String upperBound,
      String lowerBound,
      String frameType,
      boolean ignoreNulls,
      boolean upperBoundFoldable,
      boolean lowerBoundFoldable,
      long upperBoundOffset,
      long lowerBoundOffset,
      ExpressionNode upperBoundRefNode,
      ExpressionNode lowerBoundRefNode,
      boolean isPreComputeRangeFrameUpperBound,
      boolean isPreComputeRangeFrameLowerBound) {
    this.functionId = functionId;
    this.expressionNodes.addAll(expressionNodes);
    this.columnName = columnName;
    this.outputTypeNode = outputTypeNode;
    this.upperBound = upperBound;
    this.lowerBound = lowerBound;
    this.frameType = frameType;
    this.ignoreNulls = ignoreNulls;
    this.upperBoundFoldable = upperBoundFoldable;
    this.lowerBoundFoldable = lowerBoundFoldable;
    this.upperBoundOffset = upperBoundOffset;
    this.lowerBoundOffset = lowerBoundOffset;
    this.upperBoundRefNode = upperBoundRefNode;
    this.lowerBoundRefNode = lowerBoundRefNode;
    this.isPreComputeRangeFrameUpperBound = isPreComputeRangeFrameUpperBound;
    this.isPreComputeRangeFrameLowerBound = isPreComputeRangeFrameLowerBound;
  }

  private Expression.WindowFunction.Bound.Builder setBound(
      Expression.WindowFunction.Bound.Builder builder,
      String boundType,
      boolean foldable,
      long offset,
      ExpressionNode refNode,
      boolean isPreComputeRangeFrameBound) {
    switch (boundType) {
      case ("CURRENT ROW"):
        Expression.WindowFunction.Bound.CurrentRow.Builder currentRowBuilder =
            Expression.WindowFunction.Bound.CurrentRow.newBuilder();
        builder.setCurrentRow(currentRowBuilder.build());
        break;
      case ("UNBOUNDED PRECEDING"):
        Expression.WindowFunction.Bound.Unbounded_Preceding.Builder precedingBuilder =
            Expression.WindowFunction.Bound.Unbounded_Preceding.newBuilder();
        builder.setUnboundedPreceding(precedingBuilder.build());
        break;
      case ("UNBOUNDED FOLLOWING"):
        Expression.WindowFunction.Bound.Unbounded_Following.Builder followingBuilder =
            Expression.WindowFunction.Bound.Unbounded_Following.newBuilder();
        builder.setUnboundedFollowing(followingBuilder.build());
        break;
      default:
        if (isPreComputeRangeFrameBound) {
          // Used only when backend is velox and frame type is RANGE.
          if (!frameType.equals("RANGE")) {
            throw new GlutenException(
                "Only Range frame supports PreComputeRangeFrameBound, but got " + frameType);
          }
          if (offset < 0) {
            Expression.WindowFunction.Bound.Preceding.Builder refPrecedingBuilder =
                Expression.WindowFunction.Bound.Preceding.newBuilder();
            refPrecedingBuilder.setRef(refNode.toProtobuf());
            builder.setPreceding(refPrecedingBuilder.build());
          } else {
            Expression.WindowFunction.Bound.Following.Builder refFollowingBuilder =
                Expression.WindowFunction.Bound.Following.newBuilder();
            refFollowingBuilder.setRef(refNode.toProtobuf());
            builder.setFollowing(refFollowingBuilder.build());
          }
        } else if (foldable) {
          // Used when
          // 1. Velox backend and frame type is ROW
          // 2. Clickhouse backend
          if (offset < 0) {
            Expression.WindowFunction.Bound.Preceding.Builder offsetPrecedingBuilder =
                Expression.WindowFunction.Bound.Preceding.newBuilder();
            offsetPrecedingBuilder.setOffset(0 - offset);
            builder.setPreceding(offsetPrecedingBuilder.build());
          } else {
            Expression.WindowFunction.Bound.Following.Builder offsetFollowingBuilder =
                Expression.WindowFunction.Bound.Following.newBuilder();
            offsetFollowingBuilder.setOffset(offset);
            builder.setFollowing(offsetFollowingBuilder.build());
          }
        } else {
          throw new UnsupportedOperationException(
              "Unsupported Window Function Frame Bound Type: " + boundType);
        }
    }
    return builder;
  }

  private WindowType getWindowType(String type) {
    WindowType windowType;
    switch (type) {
      case ("ROWS"):
        windowType = WindowType.forNumber(0);
        break;
      case ("RANGE"):
        windowType = WindowType.forNumber(1);
        break;
      default:
        throw new UnsupportedOperationException("Only support ROWS and RANGE Frame type.");
    }
    return windowType;
  }

  public Expression.WindowFunction toProtobuf() {
    Expression.WindowFunction.Builder windowBuilder = Expression.WindowFunction.newBuilder();
    windowBuilder.setFunctionReference(functionId);
    if (ignoreNulls) {
      FunctionOption option = FunctionOption.newBuilder().setName("ignoreNulls").build();
      windowBuilder.addOptions(option);
    }
    for (ExpressionNode expressionNode : expressionNodes) {
      FunctionArgument.Builder functionArgument = FunctionArgument.newBuilder();
      functionArgument.setValue(expressionNode.toProtobuf());
      windowBuilder.addArguments(functionArgument.build());
    }
    windowBuilder.setOutputType(outputTypeNode.toProtobuf());

    windowBuilder.setColumnName(columnName);
    // Set upper bound and lower bound
    Expression.WindowFunction.Bound.Builder lowerBoundBuilder =
        Expression.WindowFunction.Bound.newBuilder();

    Expression.WindowFunction.Bound.Builder upperBoundBuilder =
        Expression.WindowFunction.Bound.newBuilder();
    windowBuilder.setLowerBound(
        setBound(
                lowerBoundBuilder,
                lowerBound,
                lowerBoundFoldable,
                lowerBoundOffset,
                lowerBoundRefNode,
                isPreComputeRangeFrameLowerBound)
            .build());
    windowBuilder.setUpperBound(
        setBound(
                upperBoundBuilder,
                upperBound,
                upperBoundFoldable,
                upperBoundOffset,
                upperBoundRefNode,
                isPreComputeRangeFrameUpperBound)
            .build());
    windowBuilder.setWindowType(getWindowType(frameType));
    return windowBuilder.build();
  }
}
