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
import org.apache.spark.softaffinity.SoftAffinity;
import org.apache.spark.sql.execution.datasources.FilePartition;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class RawSplitInfo implements SplitInfo {

  private final FilePartition filePartition;
  private final StructType partitionSchema;
  private final LocalFilesNode.ReadFileFormat fileFormat;
  private final List<String> metadataColumn;
  private final Map<String, String> properties;

  public RawSplitInfo(
      FilePartition filePartition,
      StructType partitionSchema,
      LocalFilesNode.ReadFileFormat fileFormat,
      List<String> metadataColumn,
      Map<String, String> properties) {
    this.filePartition = filePartition;
    this.partitionSchema = partitionSchema;
    this.fileFormat = fileFormat;
    this.metadataColumn = metadataColumn;
    this.properties = properties;
  }

  public FilePartition getFilePartition() {
    return filePartition;
  }

  public StructType getPartitionSchema() {
    return partitionSchema;
  }

  public LocalFilesNode.ReadFileFormat getFileFormat() {
    return fileFormat;
  }

  public List<String> getMetadataColumn() {
    return metadataColumn;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public List<String> preferredLocations() {
    return Arrays.asList(SoftAffinity.getFilePartitionLocations(filePartition));
  }

  @Override
  public MessageOrBuilder toProtobuf() {
    throw new GlutenException("RawSpiltInfo.toProtobuf should not be called");
  }
}
