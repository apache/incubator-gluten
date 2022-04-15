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

package io.glutenproject.substrait.plan;

import io.glutenproject.substrait.SubstraitContext;
import io.glutenproject.substrait.extensions.FunctionMappingNode;
import io.glutenproject.substrait.extensions.MappingBuilder;
import io.glutenproject.substrait.extensions.MappingNode;
import io.glutenproject.substrait.rel.RelNode;

import java.util.ArrayList;
import java.util.Map;

public class PlanBuilder {
    private PlanBuilder() {}

    public static PlanNode makePlan(ArrayList<MappingNode> mappingNodes,
                                    ArrayList<RelNode> relNodes,
                                    ArrayList<String> outNames) {
        return new PlanNode(mappingNodes, relNodes, outNames);
    }

    public static PlanNode makePlan(SubstraitContext subCtx, ArrayList<RelNode> relNodes,
                                    ArrayList<String> outNames) {
        if (subCtx == null) {
            throw new NullPointerException("ColumnarWholestageTransformer cannot doTansform.");
        }
        ArrayList<MappingNode> mappingNodes = new ArrayList<>();

        for (Map.Entry<String, Long> entry : subCtx.registeredFunction().entrySet()) {
            FunctionMappingNode mappingNode =
                    MappingBuilder.makeFunctionMapping(entry.getKey(), entry.getValue());
            mappingNodes.add(mappingNode);
        }
        return makePlan(mappingNodes, relNodes, outNames);
    }

    public static PlanNode makePlan(SubstraitContext subCtx, ArrayList<RelNode> relNodes) {
        return makePlan(subCtx, relNodes, new ArrayList<>());
    }
}
