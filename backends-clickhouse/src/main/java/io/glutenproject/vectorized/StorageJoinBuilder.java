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

package io.glutenproject.vectorized;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

import io.glutenproject.execution.BroadCastHashJoinContext;
import io.glutenproject.expression.ConverterUtils$;
import io.glutenproject.substrait.type.TypeNode;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;

public class StorageJoinBuilder implements AutoCloseable {

  public static native int nativeCachedHashTableCount();

  public static native void nativeCleanBuildHashTable(String hashTableId, long hashTableData);

  private ShuffleInputStream in;

  private int customizeBufferSize;

  private BroadCastHashJoinContext broadCastContext;

  private List<Expression> newBuildKeys;

  private List<Attribute> newOutput;

  public StorageJoinBuilder(ShuffleInputStream in,
                            BroadCastHashJoinContext broadCastContext,
                            int customizeBufferSize,
                            List<Attribute> newOutput,
                            List<Expression> newBuildKeys) {
    this.in = in;
    this.broadCastContext = broadCastContext;
    this.newOutput = newOutput;
    this.newBuildKeys = newBuildKeys;
    this.customizeBufferSize = customizeBufferSize;
  }

  private native long nativeBuild(String buildHashTableId,
                                  ShuffleInputStream in,
                                  int customizeBufferSize,
                                  String joinKeys,
                                  String joinType,
                                  byte[] namedStruct);

  /**
   * build storage join object
   */
  public long build() {
    ConverterUtils$ converter = ConverterUtils$.MODULE$;
    String join = converter.convertJoinType(broadCastContext.joinType());
    List<Expression> keys = null;
    List<Attribute> output = null;
    if (newBuildKeys.isEmpty()) {
      keys = JavaConverters.<Expression>seqAsJavaList(broadCastContext.buildSideJoinKeys());
      output = JavaConverters.<Attribute>seqAsJavaList(broadCastContext.buildSideStructure());
    } else {
      keys = newBuildKeys;
      output = newOutput;
    }
    String joinKey = keys.stream().map((Expression key) -> {
      Attribute attr = converter.getAttrFromExpr(key, false);
      return converter.genColumnNameWithExprId(attr);
    }).collect(Collectors.joining(","));

    // create table named struct
    ArrayList<TypeNode> typeList = new ArrayList<>();
    ArrayList<String> nameList = new ArrayList<>();
    for (Attribute attr : output) {
      typeList.add(converter.getTypeNode(attr.dataType(), attr.nullable()));
      nameList.add(converter.genColumnNameWithExprId(attr));
    }
    Type.Struct.Builder structBuilder = Type.Struct.newBuilder();
    for (TypeNode typeNode : typeList) {
      structBuilder.addTypes(typeNode.toProtobuf());
    }
    NamedStruct.Builder nStructBuilder = NamedStruct.newBuilder();
    nStructBuilder.setStruct(structBuilder.build());
    for (String name : nameList) {
      nStructBuilder.addNames(name);
    }
    byte[] structure = nStructBuilder.build().toByteArray();
    return nativeBuild(
        broadCastContext.buildHashTableId(),
        in,
        this.customizeBufferSize,
        joinKey,
        join,
        structure);
  }

  @Override
  public void close() throws Exception {
    try {
      in.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
