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
package org.apache.gluten.vectorized;

import org.apache.gluten.runtime.Runtime;
import org.apache.gluten.runtime.RuntimeAware;

import java.io.IOException;

public class ShuffleWriterJniWrapper implements RuntimeAware {
  private final Runtime runtime;

  private ShuffleWriterJniWrapper(Runtime runtime) {
    this.runtime = runtime;
  }

  public static ShuffleWriterJniWrapper create(Runtime runtime) {
    return new ShuffleWriterJniWrapper(runtime);
  }

  @Override
  public long rtHandle() {
    return runtime.getHandle();
  }

  /**
   * Construct native shuffle writer for shuffled RecordBatch over
   *
   * @param bufferSize size of native buffers held by each partition writer
   * @param mergeBufferSize maximum size of the merged buffer
   * @param mergeThreshold threshold to control whether native partition buffer need to be merged
   * @param codec compression codec
   * @param codecBackend HW backend for offloading compression
   * @param dataFile acquired from spark IndexShuffleBlockResolver
   * @param subDirsPerLocalDir SparkConf spark.diskStore.subDirectories
   * @param localDirs configured local directories where Spark can write files
   * @return native shuffle writer instance handle if created successfully.
   */
  public long make(
      String shortName,
      int numPartitions,
      int bufferSize,
      int mergeBufferSize,
      double mergeThreshold,
      String codec,
      String codecBackend,
      int compressionLevel,
      int compressionBufferSize,
      int diskWriteBufferSize,
      int bufferCompressThreshold,
      String compressionMode,
      int sortBufferInitialSize,
      boolean useRadixSort,
      String dataFile,
      int subDirsPerLocalDir,
      String localDirs,
      double reallocThreshold,
      long handle,
      long taskAttemptId,
      int startPartitionId,
      String shuffleWriterType) {
    return nativeMake(
        shortName,
        numPartitions,
        bufferSize,
        mergeBufferSize,
        mergeThreshold,
        codec,
        codecBackend,
        compressionLevel,
        compressionBufferSize,
        diskWriteBufferSize,
        bufferCompressThreshold,
        compressionMode,
        sortBufferInitialSize,
        useRadixSort,
        dataFile,
        subDirsPerLocalDir,
        localDirs,
        reallocThreshold,
        handle,
        taskAttemptId,
        startPartitionId,
        0,
        0,
        null,
        "local",
        shuffleWriterType);
  }

  /**
   * Construct RSS native shuffle writer for shuffled RecordBatch over
   *
   * @param bufferSize size of native buffers hold by partition writer
   * @param codec compression codec
   * @return native shuffle writer instance handle if created successfully.
   */
  public long makeForRSS(
      String shortName,
      int numPartitions,
      int bufferSize,
      String codec,
      int compressionLevel,
      int compressionBufferSize,
      int diskWriteBufferSize,
      int bufferCompressThreshold,
      String compressionMode,
      int sortBufferInitialSize,
      boolean useRadixSort,
      int pushBufferMaxSize,
      long sortBufferMaxSize,
      Object pusher,
      long handle,
      long taskAttemptId,
      int startPartitionId,
      String partitionWriterType,
      String shuffleWriterType,
      double reallocThreshold) {
    return nativeMake(
        shortName,
        numPartitions,
        bufferSize,
        0,
        0,
        codec,
        null,
        compressionLevel,
        compressionBufferSize,
        diskWriteBufferSize,
        bufferCompressThreshold,
        compressionMode,
        sortBufferInitialSize,
        useRadixSort,
        null,
        0,
        null,
        reallocThreshold,
        handle,
        taskAttemptId,
        startPartitionId,
        pushBufferMaxSize,
        sortBufferMaxSize,
        pusher,
        partitionWriterType,
        shuffleWriterType);
  }

  public native long nativeMake(
      String shortName,
      int numPartitions,
      int bufferSize,
      int mergeBufferSize,
      double mergeThreshold,
      String codec,
      String codecBackend,
      int compressionLevel,
      int compressionBufferSize,
      int diskWriteBufferSize,
      int bufferCompressThreshold,
      String compressionMode,
      int sortBufferInitialSize,
      boolean useRadixSort,
      String dataFile,
      int subDirsPerLocalDir,
      String localDirs,
      double reallocThreshold,
      long handle,
      long taskAttemptId,
      int startPartitionId,
      int pushBufferMaxSize,
      long sortBufferMaxSize,
      Object pusher,
      String partitionWriterType,
      String shuffleWriterType);

  /**
   * Evict partition data.
   *
   * @param shuffleWriterHandle shuffle writer instance handle
   * @param size expected size to Evict (in bytes)
   * @param callBySelf whether the caller is the shuffle writer itself, true when running out of
   *     off-heap memory due to allocations from the evaluator itself
   * @return actual spilled size
   */
  public native long nativeEvict(long shuffleWriterHandle, long size, boolean callBySelf)
      throws RuntimeException;

  /**
   * Split one record batch represented by bufAddrs and bufSizes into several batches. The batch is
   * split according to the first column as partition id.
   *
   * @param shuffleWriterHandle shuffle writer instance handle
   * @param numRows Rows per batch
   * @param handler handler of Velox Vector
   * @param memLimit memory usage limit for the split operation FIXME setting a cap to pool /
   *     allocator instead
   * @return batch bytes.
   */
  public native long write(long shuffleWriterHandle, int numRows, long handler, long memLimit);

  /**
   * Write the data remained in the buffers hold by native shuffle writer to each partition's
   * temporary file. And stop processing splitting
   *
   * @param shuffleWriterHandle shuffle writer instance handle
   * @return GlutenSplitResult
   */
  public native GlutenSplitResult stop(long shuffleWriterHandle) throws IOException;

  /**
   * Release resources associated with designated shuffle writer instance.
   *
   * @param shuffleWriterHandle shuffle writer instance handle
   */
  public native void close(long shuffleWriterHandle);
}
