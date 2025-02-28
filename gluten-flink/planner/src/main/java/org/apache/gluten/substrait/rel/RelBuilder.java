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
package org.apache.gluten.substrait.rel;

import org.apache.gluten.substrait.SubstraitContext;
import org.apache.gluten.substrait.expression.ExpressionNode;
import org.apache.gluten.substrait.extensions.AdvancedExtensionNode;
import org.apache.gluten.substrait.type.ColumnTypeNode;
import org.apache.gluten.substrait.type.TypeNode;

import io.substrait.proto.CrossRel;
import io.substrait.proto.JoinRel;
import io.substrait.proto.SortField;

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

}
