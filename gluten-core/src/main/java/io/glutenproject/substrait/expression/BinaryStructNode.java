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

import com.google.protobuf.ByteString;
import io.substrait.proto.Expression;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;

public class BinaryStructNode implements ExpressionNode, Serializable {
  // first is index, second is value
  private final byte[][] values;
  private final StructType type;

  public BinaryStructNode(byte[][] values, StructType type) {
    this.values = values;
    this.type = type;
  }

  public ExpressionNode getFieldLiteral(int index) {
    return ExpressionBuilder.makeLiteral(values[index], type.fields()[index].dataType(),
        type.fields()[index].nullable());
  }

  @Override
  public Expression toProtobuf() {
    Expression.Literal.Struct.Builder structBuilder = Expression.Literal.Struct.newBuilder();
    Expression.Literal.Builder literalBuilder = Expression.Literal.newBuilder();
    for (byte[] value : values) {
      // TODO, here we copy the binary literal, if it is long such as BloomFilter binary,
      //  it will cost much time
      literalBuilder.setBinary(ByteString.copyFrom(value));
      structBuilder.addFields(literalBuilder);
    }
    literalBuilder.setStruct(structBuilder.build());

    Expression.Builder builder = Expression.newBuilder();
    builder.setLiteral(literalBuilder.build());

    return builder.build();
  }
}

