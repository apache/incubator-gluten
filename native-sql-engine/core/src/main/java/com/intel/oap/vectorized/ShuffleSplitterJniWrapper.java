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

import java.io.IOException;

public class ShuffleSplitterJniWrapper {

  public ShuffleSplitterJniWrapper() throws IOException {
    JniUtils.getInstance();
  }

  /**
   * Construct native splitter for shuffled RecordBatch over
   *
   * @param part contains the partitioning parameter needed by native splitter
   * @param bufferSize size of native buffers hold by each partition writer
   * @param codec compression codec
   * @param dataFile acquired from spark IndexShuffleBlockResolver
   * @param subDirsPerLocalDir SparkConf spark.diskStore.subDirectories
   * @param localDirs configured local directories where Spark can write files
   * @param preferSpill
   * @param memoryPoolId
   * @return native splitter instance id if created successfully.
   */
  public long make(
      NativePartitioning part,
      int bufferSize,
      String codec,
      String dataFile,
      int subDirsPerLocalDir,
      String localDirs,
      boolean preferSpill,
      long memoryPoolId) {
    return nativeMake(
        part.getShortName(),
        part.getNumPartitions(),
        part.getSchema(),
        part.getExprList(),
        bufferSize,
        codec,
        dataFile,
        subDirsPerLocalDir,
        localDirs,
        preferSpill,
        memoryPoolId);
  }

  public native long nativeMake(
      String shortName,
      int numPartitions,
      byte[] schema,
      byte[] exprList,
      int bufferSize,
      String codec,
      String dataFile,
      int subDirsPerLocalDir,
      String localDirs,
      boolean preferSpill,
      long memoryPoolId);

  /**
   *
   * Spill partition data to disk.
   *
   * @param splitterId splitter instance id
   * @param size expected size to spill (in bytes)
   * @param callBySelf whether the caller is the shuffle splitter itself, true
   *                   when running out of off-heap memory due to allocations from
   *                   the evaluator itself
   * @return actual spilled size
   */
  public native long nativeSpill(long splitterId, long size, boolean callBySelf) throws RuntimeException;

  /**
   * Split one record batch represented by bufAddrs and bufSizes into several batches. The batch is
   * split according to the first column as partition id. During splitting, the data in native
   * buffers will be write to disk when the buffers are full.
   *
   * @param splitterId splitter instance id
   * @param numRows Rows per batch
   * @param bufAddrs Addresses of buffers
   * @param bufSizes Sizes of buffers
   * @param firstRecordBatch whether this record batch is the first
   *                         record batch in the first partition.
   * @return If the firstRecorBatch is true, return the compressed size, otherwise -1.
   */
  public native long split(
      long splitterId, int numRows, long[] bufAddrs, long[] bufSizes, boolean firstRecordBatch)
      throws IOException;

  /**
   * Update the compress type.
   */
  public native void setCompressType(long splitterId, String compressType);

  /**
   * Write the data remained in the buffers hold by native splitter to each partition's temporary
   * file. And stop processing splitting
   *
   * @param splitterId splitter instance id
   * @return SplitResult
   */
  public native SplitResult stop(long splitterId) throws IOException;

  /**
   * Release resources associated with designated splitter instance.
   *
   * @param splitterId splitter instance id
   */
  public native void close(long splitterId);
}
