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

import com.google.protobuf.Any;
import com.google.protobuf.StringValue;
import io.substrait.proto.ReadRel;

import java.io.Serializable;

public class ExtensionTableNode implements Serializable {
  private static final String MERGE_TREE = "MergeTree;";
  private Long minPartsNum;
  private Long maxPartsNum;
  private String database = null;
  private String tableName = null;
  private String relativePath = null;
  private StringBuffer extensionTableStr = new StringBuffer(MERGE_TREE);

  ExtensionTableNode(Long minPartsNum, Long maxPartsNum, String database, String tableName,
                     String relativePath) {
    this.minPartsNum = minPartsNum;
    this.maxPartsNum = maxPartsNum;
    this.database = database;
    this.tableName = tableName;
    this.relativePath = relativePath;
    // MergeTree;{database}\n{table}\n{relative_path}\n{min_part}\n{max_part}\n
    extensionTableStr.append(database).append("\n").append(tableName).append("\n")
        .append(relativePath).append("\n")
        .append(this.minPartsNum).append("\n")
        .append(this.maxPartsNum).append("\n");
  }

  public ReadRel.ExtensionTable toProtobuf() {
    ReadRel.ExtensionTable.Builder extensionTableBuilder = ReadRel.ExtensionTable.newBuilder();
    StringValue extensionTable =  StringValue.newBuilder()
            .setValue(extensionTableStr.toString()).build();
    extensionTableBuilder.setDetail(Any.pack(extensionTable));
    return extensionTableBuilder.build();
  }
}
