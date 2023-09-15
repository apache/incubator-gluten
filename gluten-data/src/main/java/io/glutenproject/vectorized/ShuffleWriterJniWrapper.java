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

import io.glutenproject.init.JniInitialized;

import java.io.IOException;

public class ShuffleWriterJniWrapper extends JniInitialized {

  public ShuffleWriterJniWrapper() {}

  /**
   * Construct native shuffle writer for shuffled RecordBatch over
   *
   * @param part contains the partitioning parameter needed by native splitter
   * @param bufferSize size of native buffers held by each partition writer
   * @param codec compression codec
   * @param codecBackend HW backend for offloading compression
   * @param dataFile acquired from spark IndexShuffleBlockResolver
   * @param subDirsPerLocalDir SparkConf spark.diskStore.subDirectories
   * @param localDirs configured local directories where Spark can write files
   * @param preferEvict if true, write the partition buffer to disk once it is full
   * @return native shuffle writer instance handle if created successfully.
   */
  public long make(
      NativePartitioning part,
      long offheapPerTask,
      int bufferSize,
      String codec,
      String codecBackend,
      int bufferCompressThreshold,
      String compressionMode,
      String dataFile,
      int subDirsPerLocalDir,
      String localDirs,
      boolean preferEvict,
      long executionCtxHandle,
      long memoryManagerHandle,
      boolean writeEOS,
      long handle,
      long taskAttemptId) {
    return nativeMake(
        part.getShortName(),
        part.getNumPartitions(),
        offheapPerTask,
        bufferSize,
        codec,
        codecBackend,
        bufferCompressThreshold,
        compressionMode,
        dataFile,
        subDirsPerLocalDir,
        localDirs,
        preferEvict,
        executionCtxHandle,
        memoryManagerHandle,
        writeEOS,
        handle,
        taskAttemptId,
        0,
        null,
        "local");
  }

  /**
   * Construct RSS native shuffle writer for shuffled RecordBatch over
   *
   * @param part contains the partitioning parameter needed by native shuffle writer
   * @param bufferSize size of native buffers hold by partition writer
   * @param codec compression codec
   * @return native shuffle writer instance handle if created successfully.
   */
  public long makeForRSS(
      NativePartitioning part,
      long offheapPerTask,
      int bufferSize,
      String codec,
      int bufferCompressThreshold,
      String compressionMode,
      int pushBufferMaxSize,
      Object pusher,
      long executionCtxHandle,
      long memoryManagerHandle,
      long handle,
      long taskAttemptId,
      String partitionWriterType) {
    return nativeMake(
        part.getShortName(),
        part.getNumPartitions(),
        offheapPerTask,
        bufferSize,
        codec,
        null,
        bufferCompressThreshold,
        compressionMode,
        null,
        0,
        null,
        true,
        executionCtxHandle,
        memoryManagerHandle,
        true,
        handle,
        taskAttemptId,
        pushBufferMaxSize,
        pusher,
        partitionWriterType);
  }

  public native long nativeMake(
      String shortName,
      int numPartitions,
      long offheapPerTask,
      int bufferSize,
      String codec,
      String codecBackend,
      int bufferCompressThreshold,
      String compressionMode,
      String dataFile,
      int subDirsPerLocalDir,
      String localDirs,
      boolean preferEvict,
      long executionCtxHandle,
      long memoryManagerHandle,
      boolean writeEOS,
      long handle,
      long taskAttemptId,
      int pushBufferMaxSize,
      Object pusher,
      String partitionWriterType);

  /**
   * Evict partition data.
   *
   * @param shuffleWriterHandle shuffle writer instance handle
   * @param size expected size to Evict (in bytes)
   * @param callBySelf whether the caller is the shuffle writer itself, true when running out of
   *     off-heap memory due to allocations from the evaluator itself
   * @return actual spilled size
   */
  public native long nativeEvict(
      long executionCtxHandle, long shuffleWriterHandle, long size, boolean callBySelf)
      throws RuntimeException;

  /**
   * Split one record batch represented by bufAddrs and bufSizes into several batches. The batch is
   * split according to the first column as partition id.
   *
   * @param shuffleWriterHandle shuffle writer instance handle
   * @param numRows Rows per batch
   * @param handler handler of Velox Vector
   * @return batch bytes.
   */
  public native long split(
      long executionCtxHandle, long shuffleWriterHandle, int numRows, long handler)
      throws IOException;

  /**
   * Write the data remained in the buffers hold by native shuffle writer to each partition's
   * temporary file. And stop processing splitting
   *
   * @param shuffleWriterHandle shuffle writer instance handle
   * @return GlutenSplitResult
   */
  public native GlutenSplitResult stop(long executionCtxHandle, long shuffleWriterHandle)
      throws IOException;

  /**
   * Release resources associated with designated shuffle writer instance.
   *
   * @param shuffleWriterHandle shuffle writer instance handle
   */
  public native void close(long executionCtxHandle, long shuffleWriterHandle);
}
