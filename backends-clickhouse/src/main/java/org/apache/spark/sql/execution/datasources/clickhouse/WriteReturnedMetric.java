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
package org.apache.spark.sql.execution.datasources.clickhouse;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WriteReturnedMetric {

  @JsonProperty("part_name")
  protected String partName;

  @JsonProperty("mark_count")
  protected long markCount;

  @JsonProperty("disk_size")
  protected long diskSize;

  @JsonProperty("row_count")
  protected long rowCount;

  @JsonProperty("bucket_id")
  protected String bucketId;

  @JsonProperty("partition_values")
  protected String partitionValues;

  public String getPartName() {
    return partName;
  }

  public void setPartName(String partName) {
    this.partName = partName;
  }

  public long getMarkCount() {
    return markCount;
  }

  public void setMarkCount(long markCount) {
    this.markCount = markCount;
  }

  public long getDiskSize() {
    return diskSize;
  }

  public void setDiskSize(long diskSize) {
    this.diskSize = diskSize;
  }

  public long getRowCount() {
    return rowCount;
  }

  public void setRowCount(long rowCount) {
    this.rowCount = rowCount;
  }

  public String getPartitionValues() {
    return partitionValues;
  }

  public void setPartitionValues(String partitionValues) {
    this.partitionValues = partitionValues;
  }

  public String getBucketId() {
    return bucketId;
  }

  public void setBucketId(String bucketId) {
    this.bucketId = bucketId;
  }
}
