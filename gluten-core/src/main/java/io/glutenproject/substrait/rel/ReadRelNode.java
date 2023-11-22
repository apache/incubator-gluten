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
package io.glutenproject.substrait.rel;

import io.glutenproject.substrait.SubstraitContext;
import io.glutenproject.substrait.expression.ExpressionNode;
import io.glutenproject.substrait.type.ColumnTypeNode;
import io.glutenproject.substrait.type.TypeNode;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelCommon;
import io.substrait.proto.Type;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReadRelNode implements RelNode, Serializable {
  private final List<TypeNode> types = new ArrayList<>();
  private final List<String> names = new ArrayList<>();
  private final List<ColumnTypeNode> columnTypeNodes = new ArrayList<>();
  private final SubstraitContext context;
  private final ExpressionNode filterNode;
  private final Long iteratorIndex;
  private StructType dataSchema;
  private Map<String, String> properties;

  ReadRelNode(
      List<TypeNode> types,
      List<String> names,
      SubstraitContext context,
      ExpressionNode filterNode,
      Long iteratorIndex) {
    this.types.addAll(types);
    this.names.addAll(names);
    this.context = context;
    this.filterNode = filterNode;
    this.iteratorIndex = iteratorIndex;
  }

  ReadRelNode(
      List<TypeNode> types,
      List<String> names,
      SubstraitContext context,
      ExpressionNode filterNode,
      Long iteratorIndex,
      List<ColumnTypeNode> columnTypeNodes) {
    this.types.addAll(types);
    this.names.addAll(names);
    this.context = context;
    this.filterNode = filterNode;
    this.iteratorIndex = iteratorIndex;
    this.columnTypeNodes.addAll(columnTypeNodes);
  }

  public void setDataSchema(StructType schema) {
    this.dataSchema = new StructType();
    for (StructField field : schema.fields()) {
      Boolean found = false;
      for (int i = 0; i < names.size(); i++) {
        // Case-insensitive schema matching
        if (field.name().equalsIgnoreCase(names.get(i))) {
          this.dataSchema =
              this.dataSchema.add(
                  names.get(i), field.dataType(), field.nullable(), field.metadata());
          found = true;
          break;
        }
      }

      if (!found) {
        this.dataSchema = this.dataSchema.add(field);
      }
    }
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public Rel toProtobuf() {
    RelCommon.Builder relCommonBuilder = RelCommon.newBuilder();
    relCommonBuilder.setDirect(RelCommon.Direct.newBuilder());

    Type.Struct.Builder structBuilder = Type.Struct.newBuilder();
    for (TypeNode typeNode : types) {
      structBuilder.addTypes(typeNode.toProtobuf());
    }

    NamedStruct.Builder nStructBuilder = NamedStruct.newBuilder();
    nStructBuilder.setStruct(structBuilder.build());
    for (String name : names) {
      nStructBuilder.addNames(name);
    }

    if (!columnTypeNodes.isEmpty()) {
      for (ColumnTypeNode columnTypeNode : columnTypeNodes) {
        nStructBuilder.addColumnTypes(columnTypeNode.toProtobuf());
      }
    }

    ReadRel.Builder readBuilder = ReadRel.newBuilder();
    readBuilder.setCommon(relCommonBuilder.build());
    readBuilder.setBaseSchema(nStructBuilder.build());
    if (filterNode != null) {
      readBuilder.setFilter(filterNode.toProtobuf());
    }
    if (this.iteratorIndex != null) {
      LocalFilesNode filesNode = context.getInputIteratorNode(iteratorIndex);
      if (dataSchema != null) {
        filesNode.setFileSchema(dataSchema);
        filesNode.setFileReadProperties(properties);
      }
      readBuilder.setLocalFiles(filesNode.toProtobuf());
    } else if (context.getSplitInfos() != null && !context.getSplitInfos().isEmpty()) {
      SplitInfo currentSplitInfo = context.getCurrentSplitInfo();
      if (currentSplitInfo instanceof LocalFilesNode) {
        LocalFilesNode filesNode = (LocalFilesNode) currentSplitInfo;
        if (dataSchema != null) {
          filesNode.setFileSchema(dataSchema);
          filesNode.setFileReadProperties(properties);
        }
        readBuilder.setLocalFiles(((LocalFilesNode) currentSplitInfo).toProtobuf());
      } else if (currentSplitInfo instanceof ExtensionTableNode) {
        readBuilder.setExtensionTable(((ExtensionTableNode) currentSplitInfo).toProtobuf());
      }
    }
    Rel.Builder builder = Rel.newBuilder();
    builder.setRead(readBuilder.build());
    return builder.build();
  }
}
