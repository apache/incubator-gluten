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

import io.glutenproject.expression.ConverterUtils$;
import io.glutenproject.substrait.SubstraitContext;
import io.glutenproject.substrait.expression.AggregateFunctionNode;
import io.glutenproject.substrait.expression.ExpressionNode;
import io.glutenproject.substrait.extensions.AdvancedExtensionNode;
import io.glutenproject.substrait.type.ColumnTypeNode;
import io.glutenproject.substrait.type.TypeNode;
import io.substrait.proto.JoinRel;
import io.substrait.proto.SortField;
import org.apache.spark.sql.catalyst.expressions.Attribute;

import java.util.ArrayList;

/**
 * Contains helper functions for constructing substrait relations.
 */
public class RelBuilder {
  private RelBuilder() {
  }

  public static RelNode makeFilterRel(RelNode input,
                                      ExpressionNode condition,
                                      SubstraitContext context,
                                      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new FilterRelNode(input, condition);
  }

  public static RelNode makeFilterRel(RelNode input,
                                      ExpressionNode condition,
                                      AdvancedExtensionNode extensionNode,
                                      SubstraitContext context,
                                      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new FilterRelNode(input, condition, extensionNode);
  }

  public static RelNode makeProjectRel(RelNode input,
                                       ArrayList<ExpressionNode> expressionNodes,
                                       SubstraitContext context,
                                       Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new ProjectRelNode(input, expressionNodes);
  }

  public static RelNode makeProjectRel(RelNode input,
                                       ArrayList<ExpressionNode> expressionNodes,
                                       AdvancedExtensionNode extensionNode,
                                       SubstraitContext context,
                                       Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new ProjectRelNode(input, expressionNodes, extensionNode);
  }

  public static RelNode makeAggregateRel(RelNode input,
                                         ArrayList<ExpressionNode> groupings,
                                         ArrayList<AggregateFunctionNode> aggregateFunctionNodes,
                                         SubstraitContext context,
                                         Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new AggregateRelNode(input, groupings, aggregateFunctionNodes);
  }

  public static RelNode makeAggregateRel(RelNode input,
                                         ArrayList<ExpressionNode> groupings,
                                         ArrayList<AggregateFunctionNode> aggregateFunctionNodes,
                                         AdvancedExtensionNode extensionNode,
                                         SubstraitContext context,
                                         Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new AggregateRelNode(input, groupings, aggregateFunctionNodes, extensionNode);
  }

  public static RelNode makeReadRel(ArrayList<TypeNode> types,
                                    ArrayList<String> names,
                                    ExpressionNode filter,
                                    SubstraitContext context,
                                    Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new ReadRelNode(types, names, context, filter, null);
  }

  public static RelNode makeReadRel(ArrayList<TypeNode> types,
                                    ArrayList<String> names,
                                    ArrayList<ColumnTypeNode> columnTypeNodes,
                                    ExpressionNode filter,
                                    SubstraitContext context,
                                    Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new ReadRelNode(types, names, context, filter, null, columnTypeNodes);
  }

  public static RelNode makeReadRel(ArrayList<TypeNode> types,
                                    ArrayList<String> names,
                                    ExpressionNode filter,
                                    Long iteratorIndex,
                                    SubstraitContext context,
                                    Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new ReadRelNode(types, names, context, filter, iteratorIndex);
  }

  public static RelNode makeReadRel(ArrayList<Attribute> attributes,
                                    SubstraitContext context,
                                    Long operatorId) {
    if (operatorId >= 0) {
      // If the operator id is negative, will not register the rel to operator.
      // Currently, only for the special handling in join.
      context.registerRelToOperator(operatorId);
    }
    ArrayList<TypeNode> typeList = new ArrayList<>();
    ArrayList<String> nameList = new ArrayList<>();
    ConverterUtils$ converter = ConverterUtils$.MODULE$;
    for (Attribute attr : attributes) {
      typeList.add(converter.getTypeNode(attr.dataType(), attr.nullable()));
      nameList.add(converter.genColumnNameWithExprId(attr));
    }

    // The iterator index will be added in the path of LocalFiles.
    Long iteratorIndex = context.nextIteratorIndex();
    context.setIteratorNode(
        iteratorIndex,
        LocalFilesBuilder.makeLocalFiles(
            converter.ITERATOR_PREFIX().concat(iteratorIndex.toString())));
    return new ReadRelNode(typeList, nameList, context, null, iteratorIndex);
  }

  public static RelNode makeJoinRel(RelNode left,
                                    RelNode right,
                                    JoinRel.JoinType joinType,
                                    ExpressionNode expression,
                                    ExpressionNode postJoinFilter,
                                    SubstraitContext context,
                                    Long operatorId) {
    context.registerRelToOperator(operatorId);
    return makeJoinRel(left, right, joinType, expression,
            postJoinFilter, null, context, operatorId);
  }

  public static RelNode makeJoinRel(RelNode left,
                                    RelNode right,
                                    JoinRel.JoinType joinType,
                                    ExpressionNode expression,
                                    ExpressionNode postJoinFilter,
                                    AdvancedExtensionNode extensionNode,
                                    SubstraitContext context,
                                    Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new JoinRelNode(left, right, joinType, expression, postJoinFilter, extensionNode);
  }

  public static RelNode makeSortRel(RelNode input,
                                    ArrayList<SortField> sorts,
                                    AdvancedExtensionNode extensionNode,
                                    SubstraitContext context,
                                    Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new SortRelNode(input, sorts, extensionNode);
  }

  public static RelNode makeSortRel(RelNode input,
                                    ArrayList<SortField> sorts,
                                    SubstraitContext context,
                                    Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new SortRelNode(input, sorts);
  }
}
