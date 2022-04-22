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
import io.substrait.proto.Expression;
import io.substrait.proto.JoinRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;

import java.io.Serializable;

public class JoinRelNode implements RelNode, Serializable {
    private final RelNode left;
    private final RelNode right;
    private final JoinRel.JoinType joinType;
    private final ExpressionNode expression;
    private final ExpressionNode postJoinFilter;

    JoinRelNode(RelNode left, RelNode right, JoinRel.JoinType joinType, ExpressionNode expression, ExpressionNode postJoinFilter) {
        this.left = left;
        this.right = right;
        this.joinType = joinType;
        this.expression = expression;
        this.postJoinFilter = postJoinFilter;
    }

    @Override
    public Rel toProtobuf() {
        RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
        relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());
        JoinRel.Builder joinBuilder = JoinRel.newBuilder();

        joinBuilder.setLeft(left.toProtobuf());
        joinBuilder.setRight(right.toProtobuf());
        joinBuilder.setType(joinType);

        if (this.expression != null) {
            joinBuilder.setExpression(expression.toProtobuf());
        }
        if (this.postJoinFilter != null) {
            joinBuilder.setPostJoinFilter(postJoinFilter.toProtobuf());
        }

        return Rel.newBuilder().setJoin(joinBuilder.build()).build();
    }
}
