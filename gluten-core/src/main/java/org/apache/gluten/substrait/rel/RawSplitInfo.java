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
package org.apache.gluten.substrait.rel;

import org.apache.gluten.exception.GlutenException;

import com.google.protobuf.MessageOrBuilder;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class RawSplitInfo implements SplitInfo {
  private FilePartition filePartition;
  private StructType partitionSchema;
  private LocalFilesNode.ReadFileFormat readFileFormat;

  private List<String> metadataColumns;

  public RawSplitInfo(
      FilePartition filePartition,
      StructType partitionSchema,
      LocalFilesNode.ReadFileFormat readFileFormat,
      List<String> metadataColumns) {
    this.filePartition = filePartition;
    this.partitionSchema = partitionSchema;
    this.readFileFormat = readFileFormat;
    this.metadataColumns = metadataColumns;
  }

  public FilePartition getFilePartition() {
    return filePartition;
  }

  public List<String> getMetadataColumns() {
    return metadataColumns;
  }

  public StructType getPartitionSchema() {
    return partitionSchema;
  }

  public LocalFilesNode.ReadFileFormat getReadFileFormat() {
    return readFileFormat;
  }

  @Override
  public List<String> preferredLocations() {
    return Arrays.asList(filePartition.preferredLocations());
  }

  @Override
  public MessageOrBuilder toProtobuf() {
    throw new GlutenException("RawSpiltInfo.toProtobuf should not be used");
  }
}
