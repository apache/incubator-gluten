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

import org.apache.gluten.substrait.type.NothingNode;
import org.apache.gluten.substrait.type.StructNode;
import org.apache.gluten.substrait.type.TypeNode;

import io.substrait.proto.Expression;
import io.substrait.proto.Expression.Literal.Builder;

import java.util.List;

public class StructLiteralNode extends LiteralNodeWithValue<List<Object>> {
  public StructLiteralNode(List<Object> struct, TypeNode typeNode) {
    super(struct, typeNode);
  }

  public LiteralNode getFieldLiteral(int index) {
    List<Object> value = getValue();
    TypeNode type = ((StructNode) getTypeNode()).getFieldTypes().get(index);
    if (value.get(index) == null) {
      return ExpressionBuilder.makeNullLiteral(type);
    }
    if (type instanceof NothingNode) {
      return ExpressionBuilder.makeNullLiteral(type);
    }

    return ExpressionBuilder.makeLiteral(value.get(index), type);
  }

  @Override
  protected void updateLiteralBuilder(Builder literalBuilder, List<Object> struct) {
    Expression.Literal.Struct.Builder structBuilder = Expression.Literal.Struct.newBuilder();
    for (int i = 0; i < ((StructNode) getTypeNode()).getFieldTypes().size(); ++i) {
      structBuilder.addFields(getFieldLiteral(i).getLiteral());
    }
    literalBuilder.setStruct(structBuilder.build());
  }
}
