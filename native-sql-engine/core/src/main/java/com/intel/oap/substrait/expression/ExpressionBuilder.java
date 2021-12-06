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
import io.substrait.*;

import java.util.ArrayList;

/**
 * Contains helper functions for constructing substrait relations.
 */
public class ExpressionBuilder {
    private ExpressionBuilder() {}

    public static DoubleLiteralNode makeLiteral(Double doubleConstant) {
        return new DoubleLiteralNode(doubleConstant);
    }

    public static ScalarFunctionNode makeScalarFunction(
            Long functionId, ArrayList<ExpressionNode> expressionNodes,
            TypeNode typeNode) {
        return new ScalarFunctionNode(functionId, expressionNodes, typeNode);
    }

    public static SelectionNode makeSelection(Integer fieldIdx) {
        return new SelectionNode(fieldIdx);
    }

    public static AggregateFunctionNode makeAggregateFunction(
            ArrayList<ExpressionNode> expressionNodes, String phase, TypeNode outputTypeNode) {
        return new AggregateFunctionNode(expressionNodes, phase, outputTypeNode);
    }

    public static AggregateFunctionNode makeAggregateFunction(
            ArrayList<ExpressionNode> expressionNodes, TypeNode outputTypeNode) {
        return new AggregateFunctionNode(expressionNodes, outputTypeNode);
    }
}