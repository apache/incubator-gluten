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

import io.substrait.proto.Expression;

import java.io.Serializable;

import com.google.protobuf.ByteString;

public class BinaryLiteralNode implements ExpressionNode, Serializable {
  private final byte[] value;

  public BinaryLiteralNode(byte[] value) {
    this.value = value;
  }

  @Override
  public Expression toProtobuf() {
    Expression.Literal.Builder binaryBuilder =
        Expression.Literal.newBuilder();
    binaryBuilder.setBinary(ByteString.copyFrom(value));

    Expression.Builder builder = Expression.newBuilder();
    builder.setLiteral(binaryBuilder.build());
    return builder.build();
  }
}
