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

import io.glutenproject.expression.ConverterUtils;
import io.glutenproject.substrait.SubstraitContext;
import io.glutenproject.substrait.expression.AggregateFunctionNode;
import io.glutenproject.substrait.expression.ExpressionNode;
import io.glutenproject.substrait.expression.WindowFunctionNode;
import io.glutenproject.substrait.extensions.AdvancedExtensionNode;
import io.glutenproject.substrait.type.ColumnTypeNode;
import io.glutenproject.substrait.type.TypeNode;

import io.substrait.proto.JoinRel;
import io.substrait.proto.SortField;
import org.apache.spark.sql.catalyst.expressions.Attribute;

import java.util.List;

/** Contains helper functions for constructing substrait relations. */
public class RelBuilder {
  private RelBuilder() {}

  public static RelNode makeFilterRel(
      RelNode input, ExpressionNode condition, SubstraitContext context, Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new FilterRelNode(input, condition);
  }

  public static RelNode makeFilterRel(
      RelNode input,
      ExpressionNode condition,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new FilterRelNode(input, condition, extensionNode);
  }

  public static RelNode makeProjectRel(
      RelNode input,
      List<ExpressionNode> expressionNodes,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new ProjectRelNode(input, expressionNodes, -1);
  }

  public static RelNode makeProjectRel(
      RelNode input,
      List<ExpressionNode> expressionNodes,
      SubstraitContext context,
      Long operatorId,
      int emitStartIndex) {
    context.registerRelToOperator(operatorId);
    return new ProjectRelNode(input, expressionNodes, emitStartIndex);
  }

  public static RelNode makeProjectRel(
      RelNode input,
      List<ExpressionNode> expressionNodes,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId,
      int emitStartIndex) {
    context.registerRelToOperator(operatorId);
    return new ProjectRelNode(input, expressionNodes, extensionNode, emitStartIndex);
  }

  public static RelNode makeAggregateRel(
      RelNode input,
      List<ExpressionNode> groupings,
      List<AggregateFunctionNode> aggregateFunctionNodes,
      List<ExpressionNode> filters,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new AggregateRelNode(input, groupings, aggregateFunctionNodes, filters);
  }

  public static RelNode makeAggregateRel(
      RelNode input,
      List<ExpressionNode> groupings,
      List<AggregateFunctionNode> aggregateFunctionNodes,
      List<ExpressionNode> filters,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new AggregateRelNode(input, groupings, aggregateFunctionNodes, filters, extensionNode);
  }

  public static RelNode makeReadRel(
      List<TypeNode> types,
      List<String> names,
      ExpressionNode filter,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new ReadRelNode(types, names, context, filter, null);
  }

  public static RelNode makeReadRel(
      List<TypeNode> types,
      List<String> names,
      List<ColumnTypeNode> columnTypeNodes,
      ExpressionNode filter,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new ReadRelNode(types, names, context, filter, null, columnTypeNodes);
  }

  public static RelNode makeReadRel(
      List<TypeNode> types,
      List<String> names,
      ExpressionNode filter,
      Long iteratorIndex,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new ReadRelNode(types, names, context, filter, iteratorIndex);
  }

  public static RelNode makeReadRel(
      List<Attribute> attributes, SubstraitContext context, Long operatorId) {
    if (operatorId >= 0) {
      // If the operator id is negative, will not register the rel to operator.
      // Currently, only for the special handling in join.
      context.registerRelToOperator(operatorId);
    }

    List<TypeNode> typeList = ConverterUtils.collectAttributeTypeNodes(attributes);
    List<String> nameList = ConverterUtils.collectAttributeNamesWithExprId(attributes);

    // The iterator index will be added in the path of LocalFiles.
    Long iteratorIndex = context.nextIteratorIndex();
    context.setIteratorNode(
        iteratorIndex,
        LocalFilesBuilder.makeLocalFiles(
            ConverterUtils.ITERATOR_PREFIX().concat(iteratorIndex.toString())));
    return new ReadRelNode(typeList, nameList, context, null, iteratorIndex);
  }

  public static RelNode makeJoinRel(
      RelNode left,
      RelNode right,
      JoinRel.JoinType joinType,
      ExpressionNode expression,
      ExpressionNode postJoinFilter,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return makeJoinRel(
        left, right, joinType, expression, postJoinFilter, null, context, operatorId);
  }

  public static RelNode makeJoinRel(
      RelNode left,
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

  public static RelNode makeExpandRel(
      RelNode input,
      List<List<ExpressionNode>> projections,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new ExpandRelNode(input, projections, extensionNode);
  }

  public static RelNode makeExpandRel(
      RelNode input,
      List<List<ExpressionNode>> projections,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new ExpandRelNode(input, projections);
  }

  public static RelNode makeSortRel(
      RelNode input,
      List<SortField> sorts,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new SortRelNode(input, sorts, extensionNode);
  }

  public static RelNode makeSortRel(
      RelNode input, List<SortField> sorts, SubstraitContext context, Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new SortRelNode(input, sorts);
  }

  public static RelNode makeFetchRel(
      RelNode input, Long offset, Long count, SubstraitContext context, Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new FetchRelNode(input, offset, count);
  }

  public static RelNode makeFetchRel(
      RelNode input,
      Long offset,
      Long count,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new FetchRelNode(input, offset, count, extensionNode);
  }

  public static RelNode makeWindowRel(
      RelNode input,
      List<WindowFunctionNode> windowFunctionNodes,
      List<ExpressionNode> partitionExpressions,
      List<SortField> sorts,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new WindowRelNode(
        input, windowFunctionNodes, partitionExpressions, sorts, extensionNode);
  }

  public static RelNode makeWindowRel(
      RelNode input,
      List<WindowFunctionNode> windowFunctionNodes,
      List<ExpressionNode> partitionExpressions,
      List<SortField> sorts,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new WindowRelNode(
        input, windowFunctionNodes,
        partitionExpressions, sorts);
  }

  public static RelNode makeGenerateRel(
      RelNode input,
      ExpressionNode generator,
      List<ExpressionNode> childOutput,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new GenerateRelNode(input, generator, childOutput);
  }

  public static RelNode makeGenerateRel(
      RelNode input,
      ExpressionNode generator,
      List<ExpressionNode> childOutput,
      AdvancedExtensionNode extensionNode,
      SubstraitContext context,
      Long operatorId) {
    context.registerRelToOperator(operatorId);
    return new GenerateRelNode(input, generator, childOutput, extensionNode);
  }
}
