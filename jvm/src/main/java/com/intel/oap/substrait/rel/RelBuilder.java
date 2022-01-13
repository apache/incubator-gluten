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

import com.intel.oap.substrait.SubstraitContext;
import com.intel.oap.substrait.expression.AggregateFunctionNode;
import com.intel.oap.substrait.expression.ExpressionNode;
import com.intel.oap.substrait.type.TypeNode;

import java.util.ArrayList;

/**
 * Contains helper functions for constructing substrait relations.
 */
public class RelBuilder {
  private RelBuilder() {}

  public static RelNode makeFilterRel(RelNode input,
                                      ExpressionNode condition,
                                      ArrayList<TypeNode> types) {
    return new FilterRelNode(input, condition, types);
  }

  public static RelNode makeProjectRel(RelNode input,
                                       ArrayList<ExpressionNode> expressionNodes,
                                       ArrayList<TypeNode> inputTypes) {
    return new ProjectRelNode(input, expressionNodes, inputTypes);
  }

  public static RelNode makeAggregateRel(RelNode input,
                                         ArrayList<Integer> groupings,
                                         ArrayList<AggregateFunctionNode> aggregateFunctionNodes,
                                         ArrayList<TypeNode> inputTypeNodes,
                                         ArrayList<TypeNode> outputTypeNodes,
                                         ArrayList<ExpressionNode> resExprNodes) {
    return new AggregateRelNode(input, null, aggregateFunctionNodes,
                                inputTypeNodes, outputTypeNodes, resExprNodes);
  }

  public static RelNode makeAggregateRel(RelNode input,
                                         ArrayList<ExpressionNode> groupings,
                                         ArrayList<AggregateFunctionNode> aggregateFunctionNodes) {
    return new AggregateRelNode(input, groupings, aggregateFunctionNodes);
  }

  public static RelNode makeReadRel(ArrayList<TypeNode> types, ArrayList<String> names,
                                    ExpressionNode filterNode) {
    return new ReadRelNode(types, names, filterNode);
  }

  public static RelNode makeReadRel(ArrayList<TypeNode> types, ArrayList<String> names,
                                    ExpressionNode filter, LocalFilesNode partNode) {
    return new ReadRelNode(types, names, filter, partNode);
  }

  public static RelNode makeReadRel(ArrayList<TypeNode> types, ArrayList<String> names,
                                    ExpressionNode filter, SubstraitContext context) {
    return new ReadRelNode(types, names, filter, context);
  }
}
