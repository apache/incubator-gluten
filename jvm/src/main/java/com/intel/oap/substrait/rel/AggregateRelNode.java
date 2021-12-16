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

package com.intel.oap.substrait.rel;

import com.intel.oap.substrait.expression.AggregateFunctionNode;
import com.intel.oap.substrait.expression.ExpressionNode;
import com.intel.oap.substrait.type.TypeNode;
import io.substrait.AggregateRel;
import io.substrait.Expression;
import io.substrait.Rel;

import java.io.Serializable;
import java.util.ArrayList;

public class AggregateRelNode implements RelNode, Serializable {
    private final RelNode input;
    private final ArrayList<Integer> groupings = new ArrayList<>();
    private final ArrayList<AggregateFunctionNode> aggregateFunctionNodes =
            new ArrayList<>();
    private final String phase;
    private final ArrayList<TypeNode> inputTypeNodes = new ArrayList<>();
    private final ArrayList<TypeNode> outputTypeNodes = new ArrayList<>();
    private final ArrayList<ExpressionNode> resExprNodes = new ArrayList<>();

    AggregateRelNode(RelNode input,
                     ArrayList<Integer> groupings,
                     ArrayList<AggregateFunctionNode> aggregateFunctionNodes,
                     ArrayList<TypeNode> inputTypeNodes,
                     ArrayList<TypeNode> outputTypeNodes,
                     ArrayList<ExpressionNode> resExprNodes) {
        this.input = input;
        this.groupings.addAll(groupings);
        this.aggregateFunctionNodes.addAll(aggregateFunctionNodes);
        this.phase = null;
        this.inputTypeNodes.addAll(inputTypeNodes);
        this.outputTypeNodes.addAll(outputTypeNodes);
        this.resExprNodes.addAll(resExprNodes);
    }

    @Override
    public Rel toProtobuf() {
        AggregateRel.Grouping.Builder groupingBuilder =
                AggregateRel.Grouping.newBuilder();
        for (Integer integer : groupings) {
            groupingBuilder.addInputFields(integer.intValue());
        }

        AggregateRel.Builder aggBuilder = AggregateRel.newBuilder();
        aggBuilder.addGroupings(groupingBuilder.build());

        for (AggregateFunctionNode aggregateFunctionNode : aggregateFunctionNodes) {
            AggregateRel.Measure.Builder measureBuilder = AggregateRel.Measure.newBuilder();
            measureBuilder.setMeasure(aggregateFunctionNode.toProtobuf());
            aggBuilder.addMeasures(measureBuilder.build());
        }
        if (input != null) {
            aggBuilder.setInput(input.toProtobuf());
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
        for (TypeNode typeNode : inputTypeNodes) {
            aggBuilder.addInputTypes(typeNode.toProtobuf());
        }
        for (TypeNode typeNode : outputTypeNodes) {
            aggBuilder.addOutputTypes(typeNode.toProtobuf());
        }
        for (ExpressionNode expressionNode : resExprNodes) {
            aggBuilder.addResultExpressions(expressionNode.toProtobuf());
        }
        Rel.Builder builder = Rel.newBuilder();
        builder.setAggregate(aggBuilder.build());
        return builder.build();
    }
}
