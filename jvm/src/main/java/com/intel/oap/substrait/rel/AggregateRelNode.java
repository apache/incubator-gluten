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
import io.substrait.proto.AggregateRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;

import java.io.Serializable;
import java.util.ArrayList;

public class AggregateRelNode implements RelNode, Serializable {
    private final RelNode input;
    private final ArrayList<ExpressionNode> groupings = new ArrayList<>();
    private final ArrayList<AggregateFunctionNode> aggregateFunctionNodes = new ArrayList<>();

    AggregateRelNode(RelNode input,
                     ArrayList<ExpressionNode> groupings,
                     ArrayList<AggregateFunctionNode> aggregateFunctionNodes) {
        this.input = input;
        this.groupings.addAll(groupings);
        this.aggregateFunctionNodes.addAll(aggregateFunctionNodes);
    }

    @Override
    public Rel toProtobuf() {
        RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
        relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());

        AggregateRel.Grouping.Builder groupingBuilder =
                AggregateRel.Grouping.newBuilder();
        for (ExpressionNode exprNode : groupings) {
            groupingBuilder.addGroupingExpressions(exprNode.toProtobuf());
        }

        AggregateRel.Builder aggBuilder = AggregateRel.newBuilder();
        aggBuilder.setCommon(relCommonBuilder.build());
        aggBuilder.addGroupings(groupingBuilder.build());

        for (AggregateFunctionNode aggregateFunctionNode : aggregateFunctionNodes) {
            AggregateRel.Measure.Builder measureBuilder = AggregateRel.Measure.newBuilder();
            measureBuilder.setMeasure(aggregateFunctionNode.toProtobuf());
            aggBuilder.addMeasures(measureBuilder.build());
        }
        if (input != null) {
            aggBuilder.setInput(input.toProtobuf());
        }

        Rel.Builder builder = Rel.newBuilder();
        builder.setAggregate(aggBuilder.build());
        return builder.build();
    }
}
