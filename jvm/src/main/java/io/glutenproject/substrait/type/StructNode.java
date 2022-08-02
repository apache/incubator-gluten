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

package io.glutenproject.substrait.type;

import io.substrait.proto.Type;

import java.io.Serializable;
import java.util.ArrayList;

public class StructNode implements TypeNode, Serializable {
  private final ArrayList<TypeNode> types = new ArrayList<>();

  StructNode(ArrayList<TypeNode> types) {
    this.types.addAll(types);
  }

  @Override
  public Type toProtobuf() {
    Type.Struct.Builder structBuilder = Type.Struct.newBuilder();
    for (TypeNode typeNode : types) {
      structBuilder.addTypes(typeNode.toProtobuf());
    }
    Type.Builder builder = Type.newBuilder();
    builder.setStruct(structBuilder.build());
    return builder.build();
  }
}
