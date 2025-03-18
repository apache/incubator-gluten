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
package org.apache.gluten.substrait.expression;

import org.apache.gluten.substrait.type.TypeNode;

import io.substrait.proto.Expression;

import java.io.Serializable;

public class CastNode implements ExpressionNode, Serializable {
  private final TypeNode typeNode;
  private final ExpressionNode expressionNode;

  public final boolean ansiEnabled;

  CastNode(TypeNode typeNode, ExpressionNode expressionNode, boolean ansiEnabled) {
    this.typeNode = typeNode;
    this.expressionNode = expressionNode;
    this.ansiEnabled = ansiEnabled;
  }

  @Override
  public Expression toProtobuf() {
    Expression.Cast.Builder castBuilder = Expression.Cast.newBuilder();
    castBuilder.setType(typeNode.toProtobuf());
    castBuilder.setInput(expressionNode.toProtobuf());
    if (ansiEnabled) {
      // Throw exception on failure.
      castBuilder.setFailureBehaviorValue(2);
    } else {
      // Return null on failure.
      castBuilder.setFailureBehaviorValue(1);
    }
    Expression.Builder builder = Expression.newBuilder();
    builder.setCast(castBuilder.build());
    return builder.build();
  }
}
