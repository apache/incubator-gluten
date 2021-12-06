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

package com.intel.oap.substrait.expression;

import com.intel.oap.substrait.expression.ExpressionNode;
import com.intel.oap.substrait.type.TypeNode;
import io.substrait.Expression;
import io.substrait.Extensions;

import java.io.Serializable;
import java.util.ArrayList;

public class AggregateFunctionNode implements Serializable {
    private final Long functionId;
    private final ArrayList<ExpressionNode> expressionNodes = new ArrayList<>();
    private final String phase;
    private final TypeNode outputTypeNode;

    AggregateFunctionNode(Long functionId, ArrayList<ExpressionNode> expressionNodes,
                          String phase, TypeNode outputTypeNode) {
        this.functionId = functionId;
        this.expressionNodes.addAll(expressionNodes);
        this.phase = phase;
        this.outputTypeNode = outputTypeNode;
    }

    AggregateFunctionNode(ArrayList<ExpressionNode> expressionNodes,
                          String phase, TypeNode outputTypeNode) {
        this.functionId = null;
        this.expressionNodes.addAll(expressionNodes);
        this.phase = phase;
        this.outputTypeNode = outputTypeNode;
    }

    AggregateFunctionNode(ArrayList<ExpressionNode> expressionNodes,
                          TypeNode outputTypeNode) {
        this.functionId = null;
        this.expressionNodes.addAll(expressionNodes);
        this.phase = null;
        this.outputTypeNode = outputTypeNode;
    }

    public Expression.AggregateFunction toProtobuf() {
        Expression.AggregateFunction.Builder aggBuilder =
                Expression.AggregateFunction.newBuilder();
        if (functionId != null) {
            Extensions.FunctionId.Builder funcIdBuilder = Extensions.FunctionId.newBuilder();
            funcIdBuilder.setId(functionId.longValue());
            aggBuilder.setId(funcIdBuilder.build());
        }
        if (phase == null) {
            aggBuilder.setPhase(Expression.AggregationPhase.UNKNOWN);
        } else {
            switch(phase) {
                case "PARTIAL":
                    aggBuilder.setPhase(Expression.AggregationPhase.INITIAL_TO_INTERMEDIATE);
                    break;
                case "PARTIAL_MERGE":
                    aggBuilder.setPhase(Expression.AggregationPhase.INTERMEDIATE_TO_INTERMEDIATE);
                    break;
                case "FINAL":
                    aggBuilder.setPhase(Expression.AggregationPhase.INTERMEDIATE_TO_RESULT);
                    break;
                default:
                    aggBuilder.setPhase(Expression.AggregationPhase.UNKNOWN);
            }
        }
        for (ExpressionNode expressionNode : expressionNodes) {
            aggBuilder.addArgs(expressionNode.toProtobuf());
        }
        aggBuilder.setOutputType(outputTypeNode.toProtobuf());

        return aggBuilder.build();
    }
}
