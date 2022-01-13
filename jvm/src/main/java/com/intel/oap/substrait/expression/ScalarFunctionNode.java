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

import com.intel.oap.substrait.type.TypeNode;
import io.substrait.proto.*;

import java.io.Serializable;
import java.util.ArrayList;

public class ScalarFunctionNode implements ExpressionNode, Serializable {
    private final Long functionId;
    private final ArrayList<ExpressionNode> expressionNodes = new ArrayList<>();
    private final TypeNode typeNode;

    ScalarFunctionNode(Long functionId, ArrayList<ExpressionNode> expressionNodes,
                       TypeNode typeNode) {
        this.functionId = functionId;
        this.expressionNodes.addAll(expressionNodes);
        this.typeNode = typeNode;
    }

    @Override
    public Expression toProtobuf() {
        Expression.ScalarFunction.Builder scalarBuilder =
                Expression.ScalarFunction.newBuilder();
        scalarBuilder.setFunctionReference(functionId.intValue());
        for (ExpressionNode expressionNode : expressionNodes) {
            scalarBuilder.addArgs(expressionNode.toProtobuf());
        }
        scalarBuilder.setOutputType(typeNode.toProtobuf());

        Expression.Builder builder = Expression.newBuilder();
        builder.setScalarFunction(scalarBuilder.build());
        return builder.build();
    }
}
