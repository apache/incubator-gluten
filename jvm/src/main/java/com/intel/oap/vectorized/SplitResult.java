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

package com.intel.oap.vectorized;

/** POJO to hold native split result */
public class SplitResult {
  private final long totalComputePidTime;
  private final long totalWriteTime;
  private final long totalSpillTime;
  private final long totalCompressTime; // overlaps with totalSpillTime and totalWriteTime
  private final long totalBytesWritten;
  private final long totalBytesSpilled;
  private final long[] partitionLengths;

  public SplitResult(
      long totalComputePidTime,
      long totalWriteTime,
      long totalSpillTime,
      long totalCompressTime,
      long totalBytesWritten,
      long totalBytesSpilled,
      long[] partitionLengths) {
    this.totalComputePidTime = totalComputePidTime;
    this.totalWriteTime = totalWriteTime;
    this.totalSpillTime = totalSpillTime;
    this.totalCompressTime = totalCompressTime;
    this.totalBytesWritten = totalBytesWritten;
    this.totalBytesSpilled = totalBytesSpilled;
    this.partitionLengths = partitionLengths;
  }

  public long getTotalComputePidTime() {
    return totalComputePidTime;
  }

  public long getTotalWriteTime() {
    return totalWriteTime;
  }

  public long getTotalSpillTime() {
    return totalSpillTime;
  }

  public long getTotalCompressTime() {
    return totalCompressTime;
  }

  public long getTotalBytesWritten() {
    return totalBytesWritten;
  }

  public long getTotalBytesSpilled() {
    return totalBytesSpilled;
  }

  public long[] getPartitionLengths() {
    return partitionLengths;
  }
}
