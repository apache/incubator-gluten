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

import io.glutenproject.substrait.type.*;
import org.apache.spark.sql.catalyst.InternalRow;

import io.substrait.proto.Expression;
import io.substrait.proto.Expression.Literal.Builder;

import java.util.ArrayList;

public class StructLiteralNode extends LiteralNodeWithValue<InternalRow> {
  public StructLiteralNode(InternalRow row, TypeNode typeNode) {
    super(row, typeNode);
  }

  public LiteralNode getFieldLiteral(int index) {
    InternalRow value = getValue();
    ArrayList<TypeNode> fieldTypes = ((StructNode) getTypeNode()).getFieldTypes();
    if (fieldTypes.get(index) instanceof BooleanTypeNode) {
      return ExpressionBuilder.makeLiteral(value.getBoolean(index), fieldTypes.get(index));
    }
    if (fieldTypes.get(index) instanceof I8TypeNode) {
      return ExpressionBuilder.makeLiteral(value.getByte(index), fieldTypes.get(index));
    }
    if (fieldTypes.get(index) instanceof I16TypeNode) {
      return ExpressionBuilder.makeLiteral(value.getShort(index), fieldTypes.get(index));
    }
    if (fieldTypes.get(index) instanceof I32TypeNode) {
      return ExpressionBuilder.makeLiteral(value.getInt(index), fieldTypes.get(index));
    }
    if (fieldTypes.get(index) instanceof I64TypeNode) {
      return ExpressionBuilder.makeLiteral(value.getLong(index), fieldTypes.get(index));
    }
    if (fieldTypes.get(index) instanceof FP32TypeNode) {
      return ExpressionBuilder.makeLiteral(value.getFloat(index), fieldTypes.get(index));
    }
    if (fieldTypes.get(index) instanceof FP64TypeNode) {
      return ExpressionBuilder.makeLiteral(value.getDouble(index), fieldTypes.get(index));
    }
    if (fieldTypes.get(index) instanceof DateTypeNode) {
      return ExpressionBuilder.makeLiteral(value.getInt(index), fieldTypes.get(index));
    }
    if (fieldTypes.get(index) instanceof TimestampTypeNode) {
      return ExpressionBuilder.makeLiteral(value.getLong(index), fieldTypes.get(index));
    }
    if (fieldTypes.get(index) instanceof StringTypeNode) {
      return ExpressionBuilder.makeLiteral(value.getUTF8String(index), fieldTypes.get(index));
    }
    if (fieldTypes.get(index) instanceof BinaryTypeNode) {
      return ExpressionBuilder.makeLiteral(value.getBinary(index), fieldTypes.get(index));
    }
    if (fieldTypes.get(index) instanceof DecimalTypeNode) {
      return ExpressionBuilder.makeLiteral(
              value.getDecimal(index, ((DecimalTypeNode) fieldTypes.get(index)).precision,
                      ((DecimalTypeNode) fieldTypes.get(index)).scale),
              fieldTypes.get(index));
    }
    if (fieldTypes.get(index) instanceof ListNode) {
      return ExpressionBuilder.makeLiteral(value.getArray(index), fieldTypes.get(index));
    }
    if (fieldTypes.get(index) instanceof MapNode) {
      return ExpressionBuilder.makeLiteral(value.getMap(index), fieldTypes.get(index));
    }
    if (fieldTypes.get(index) instanceof StructNode) {
      return ExpressionBuilder.makeLiteral(value.getStruct(index,
              ((StructNode) fieldTypes.get(index)).getFieldTypes().size()), fieldTypes.get(index));
    }
    throw new UnsupportedOperationException(
            fieldTypes.get(index).toString() + " is not supported in getFieldLiteral.");
  }

  @Override
  protected void updateLiteralBuilder(Builder literalBuilder, InternalRow row) {
    Expression.Literal.Struct.Builder structBuilder = Expression.Literal.Struct.newBuilder();
    for (int i = 0; i < ((StructNode) getTypeNode()).getFieldTypes().size(); ++i) {
      structBuilder.addFields(getFieldLiteral(i).getLiteral());
    }
    literalBuilder.setStruct(structBuilder.build());
  }
}

