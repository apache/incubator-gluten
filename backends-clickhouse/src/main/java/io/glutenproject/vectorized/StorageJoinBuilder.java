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

import io.glutenproject.execution.BroadCastHashJoinContext;
import io.glutenproject.expression.ConverterUtils;
import io.glutenproject.expression.ConverterUtils$;
import io.glutenproject.substrait.type.TypeNode;
import io.glutenproject.utils.SubstraitUtil;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.storage.CHShuffleReadStreamFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

public class StorageJoinBuilder {

  public static native void nativeCleanBuildHashTable(String hashTableId, long hashTableData);

  public static native long nativeCloneBuildHashTable(long hashTableData);

  private static native long nativeBuild(
      String buildHashTableId,
      ShuffleInputStream in,
      String joinKeys,
      int joinType,
      byte[] namedStruct);

  private StorageJoinBuilder() {}

  /** build storage join object */
  public static long build(
      byte[] batches,
      BroadCastHashJoinContext broadCastContext,
      List<Expression> newBuildKeys,
      List<Attribute> newOutput) {
    ShuffleInputStream in = CHShuffleReadStreamFactory.create(batches, true);
    try {
      ConverterUtils$ converter = ConverterUtils$.MODULE$;
      List<Expression> keys;
      List<Attribute> output;
      if (newBuildKeys.isEmpty()) {
        keys = JavaConverters.<Expression>seqAsJavaList(broadCastContext.buildSideJoinKeys());
        output = JavaConverters.<Attribute>seqAsJavaList(broadCastContext.buildSideStructure());
      } else {
        keys = newBuildKeys;
        output = newOutput;
      }
      String joinKey =
          keys.stream()
              .map(
                  (Expression key) -> {
                    Attribute attr = converter.getAttrFromExpr(key, false);
                    return converter.genColumnNameWithExprId(attr);
                  })
              .collect(Collectors.joining(","));
      return nativeBuild(
          broadCastContext.buildHashTableId(),
          in,
          joinKey,
          SubstraitUtil.toSubstrait(broadCastContext.joinType()).ordinal(),
          toNameStruct(output).toByteArray());
    } finally {
      in.close();
    }
  }

  /** create table named struct */
  private static NamedStruct toNameStruct(List<Attribute> output) {
    ArrayList<TypeNode> typeList = ConverterUtils.collectAttributeTypeNodes(output);
    ArrayList<String> nameList = ConverterUtils.collectAttributeNamesWithExprId(output);
    Type.Struct.Builder structBuilder = Type.Struct.newBuilder();
    for (TypeNode typeNode : typeList) {
      structBuilder.addTypes(typeNode.toProtobuf());
    }
    NamedStruct.Builder nStructBuilder = NamedStruct.newBuilder();
    nStructBuilder.setStruct(structBuilder.build());
    for (String name : nameList) {
      nStructBuilder.addNames(name);
    }
    return nStructBuilder.build();
  }
}
