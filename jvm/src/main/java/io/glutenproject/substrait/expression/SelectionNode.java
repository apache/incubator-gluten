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
import java.util.ArrayList;

public class SelectionNode implements ExpressionNode, Serializable {
  private final Integer fieldIdx;
  private final Integer childFieldIdx;

  SelectionNode(Integer fieldIdx) {
    this.fieldIdx = fieldIdx;
    this.childFieldIdx = null;
  }

  SelectionNode(Integer fieldIdx, Integer childFieldIdx) {
    this.fieldIdx = fieldIdx;
    this.childFieldIdx = childFieldIdx;
  }

  @Override
  public Expression toProtobuf() {
    Expression.ReferenceSegment.StructField.Builder structBuilder =
        Expression.ReferenceSegment.StructField.newBuilder();
    structBuilder.setField(fieldIdx);

    // Handle the child field indices.
    if (childFieldIdx != null) {
      Expression.ReferenceSegment.StructField.Builder childStructBuilder =
          Expression.ReferenceSegment.StructField.newBuilder();
      childStructBuilder.setField(childFieldIdx);
      Expression.ReferenceSegment.Builder childRefBuilder =
          Expression.ReferenceSegment.newBuilder();
      childRefBuilder.setStructField(childStructBuilder.build());
      structBuilder.setChild(childRefBuilder.build());
    }

    Expression.ReferenceSegment.Builder refBuilder =
        Expression.ReferenceSegment.newBuilder();
    refBuilder.setStructField(structBuilder.build());

    Expression.FieldReference.Builder fieldBuilder =
        Expression.FieldReference.newBuilder();
    fieldBuilder.setDirectReference(refBuilder.build());

    Expression.Builder builder = Expression.newBuilder();
    builder.setSelection(fieldBuilder.build());
    return builder.build();
  }
}
