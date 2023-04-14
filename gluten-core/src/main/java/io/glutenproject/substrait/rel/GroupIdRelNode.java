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

package io.glutenproject.substrait.rel;

import io.glutenproject.substrait.expression.ExpressionNode;
import io.glutenproject.substrait.extensions.AdvancedExtensionNode;
import io.substrait.proto.GroupIdRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;

import java.io.Serializable;
import java.util.ArrayList;

public class GroupIdRelNode implements RelNode, Serializable {
  private final RelNode input;
  private final String groupName;
  private final ArrayList<ArrayList<ExpressionNode>> groupings = new ArrayList<>();

  private final ArrayList<ExpressionNode> aggExpressions = new ArrayList<>();

  private final AdvancedExtensionNode extensionNode;

  public GroupIdRelNode(RelNode input, String groupName,
                       ArrayList<ArrayList<ExpressionNode>> groupings,
                       ArrayList<ExpressionNode> aggExpressions,
                       AdvancedExtensionNode extensionNode) {
    this.input = input;
    this.groupName = groupName;
    this.groupings.addAll(groupings);
    this.aggExpressions.addAll(aggExpressions);
    this.extensionNode = extensionNode;
  }

  public GroupIdRelNode(RelNode input, String groupName,
                       ArrayList<ArrayList<ExpressionNode>> groupings,
                       ArrayList<ExpressionNode> aggExpressions) {
    this.input = input;
    this.groupName = groupName;
    this.groupings.addAll(groupings);
    this.aggExpressions.addAll(aggExpressions);
    this.extensionNode = null;
  }

  @Override
  public Rel toProtobuf() {
    RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());

    GroupIdRel.Builder groupIdBuilder = GroupIdRel.newBuilder();
    groupIdBuilder.setCommon(relCommonBuilder.build());

    if (input != null) {
      groupIdBuilder.setInput(input.toProtobuf());
    }

    for (ArrayList<ExpressionNode> groupList: groupings) {
      GroupIdRel.GroupSets.Builder groupingBuilder =
        GroupIdRel.GroupSets.newBuilder();
      for (ExpressionNode exprNode : groupList) {
        groupingBuilder.addGroupSetsExpressions(exprNode.toProtobuf());
      }
      groupIdBuilder.addGroupings(groupingBuilder.build());
    }

    for (ExpressionNode aggExpression: aggExpressions) {
      groupIdBuilder.addAggregateExpressions(aggExpression.toProtobuf());
    }

    if (extensionNode != null) {
      groupIdBuilder.setAdvancedExtension(extensionNode.toProtobuf());
    }

    groupIdBuilder.setGroupName(groupName);

    Rel.Builder builder = Rel.newBuilder();
    builder.setGroupId(groupIdBuilder.build());
    return builder.build();
  }
}
