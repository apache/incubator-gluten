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

package io.glutenproject.substrait.expression;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

import io.substrait.proto.Expression;
import io.substrait.proto.Expression.Literal.Builder;
import io.glutenproject.substrait.type.TypeNode;
import io.glutenproject.substrait.type.StructNode;

import java.util.ArrayList;

public class StructLiteralNode extends LiteralNodeWithValue<GenericInternalRow> {
  public StructLiteralNode(GenericInternalRow row, TypeNode typeNode) {
    super(row, typeNode);
  }

  public LiteralNode getFieldLiteral(int index) {
    Object[] values = getValue().values();
    ArrayList<TypeNode> fieldTypes = ((StructNode) getTypeNode()).getFieldTypes();
    return ExpressionBuilder.makeLiteral(values[index], fieldTypes.get(index));
  }

  @Override
  protected void updateLiteralBuilder(Builder literalBuilder, GenericInternalRow row) {
    ArrayList<TypeNode> fieldTypes = ((StructNode) getTypeNode()).getFieldTypes();
    Object[] values = row.values();

    Expression.Literal.Struct.Builder structBuilder = Expression.Literal.Struct.newBuilder();
    for (int i=0; i<values.length; ++i) {
      LiteralNode elementNode = ExpressionBuilder.makeLiteral(values[i], fieldTypes.get(i));
      Expression.Literal element = elementNode.getLiteral();
      structBuilder.addFields(element);
    }

    literalBuilder.setStruct(structBuilder.build());
  }
}

