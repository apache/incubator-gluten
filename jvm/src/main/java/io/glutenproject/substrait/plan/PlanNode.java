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

import io.glutenproject.substrait.extensions.FunctionMappingNode;
import io.glutenproject.substrait.rel.RelNode;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.RelRoot;

import java.io.Serializable;
import java.util.ArrayList;

public class PlanNode implements Serializable {
    private final ArrayList<FunctionMappingNode> mappingNodes = new ArrayList<>();
    private final ArrayList<RelNode> relNodes = new ArrayList<>();
    private final ArrayList<String> outNames = new ArrayList<>();

    PlanNode(ArrayList<FunctionMappingNode> mappingNodes,
             ArrayList<RelNode> relNodes,
             ArrayList<String> outNames) {
        this.mappingNodes.addAll(mappingNodes);
        this.relNodes.addAll(relNodes);
        this.outNames.addAll(outNames);
    }

    public Plan toProtobuf() {
        Plan.Builder planBuilder = Plan.newBuilder();
        // add the extension functions
        for (FunctionMappingNode mappingNode : mappingNodes) {
            planBuilder.addExtensions(mappingNode.toProtobuf());
        }

        for (RelNode relNode : relNodes) {
            PlanRel.Builder planRelBuilder = PlanRel.newBuilder();

            RelRoot.Builder relRootBuilder = RelRoot.newBuilder();
            relRootBuilder.setInput(relNode.toProtobuf());
            for (String name : outNames) {
                relRootBuilder.addNames(name);
            }
            planRelBuilder.setRoot(relRootBuilder.build());

            planBuilder.addRelations(planRelBuilder.build());
        }
        return planBuilder.build();
    }
}
