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

  public ShuffleWriterJniWrapper() {
  }

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
   * @param memoryPoolId
   * @return native shuffle writer instance id if created successfully.
   */
  public long make(NativePartitioning part, long offheapPerTask, int bufferSize, String codec,
                   String codecBackend, int batchCompressThreshold, String dataFile,
                   int subDirsPerLocalDir, String localDirs, boolean preferEvict, long memoryPoolId,
                   boolean writeSchema, long handle, long taskAttemptId) {
    return nativeMake(part.getShortName(), part.getNumPartitions(),
        offheapPerTask, bufferSize, codec, codecBackend, batchCompressThreshold, dataFile,
        subDirsPerLocalDir, localDirs, preferEvict, memoryPoolId,
        writeSchema, handle, taskAttemptId, 0, null, "local");
  }

  /**
   * Construct RSS native shuffle writer for shuffled RecordBatch over
   *
   * @param part contains the partitioning parameter needed by native shuffle writer
   * @param bufferSize size of native buffers hold by partition writer
   * @param codec compression codec
   * @param memoryPoolId
   * @return native shuffle writer instance id if created successfully.
   */
  public long makeForRSS(NativePartitioning part, long offheapPerTask,
                         int bufferSize, String codec, int batchCompressThreshold,
                         int pushBufferMaxSize, Object pusher,
                         long memoryPoolId, long handle,
                         long taskAttemptId, String partitionWriterType) {
    return nativeMake(part.getShortName(), part.getNumPartitions(),
        offheapPerTask, bufferSize, codec, null, batchCompressThreshold, null,
        0, null, true, memoryPoolId,
        false, handle, taskAttemptId, pushBufferMaxSize, pusher, partitionWriterType);
  }

  public native long nativeMake(String shortName, int numPartitions,
                                long offheapPerTask, int bufferSize,
                                String codec, String codecBackend, int batchCompressThreshold,
                                String dataFile, int subDirsPerLocalDir, String localDirs,
                                boolean preferEvict, long memoryPoolId, boolean writeSchema,
                                long handle, long taskAttemptId, int pushBufferMaxSize,
                                Object pusher, String partitionWriterType);

  /**
   * Evict partition data.
   *
   * @param shuffleWriterId shuffle writer instance id
   * @param size expected size to Evict (in bytes)
   * @param callBySelf whether the caller is the shuffle writer itself, true
   * when running out of off-heap memory due to allocations from
   * the evaluator itself
   * @return actual spilled size
   */
  public native long nativeEvict(
      long shuffleWriterId, long size, boolean callBySelf) throws RuntimeException;

  /**
   * Split one record batch represented by bufAddrs and bufSizes into several batches. The batch is
   * split according to the first column as partition id.
   *
   * @param shuffleWriterId shuffle writer instance id
   * @param numRows Rows per batch
   * @param handler handler of Velox Vector
   * @return batch bytes.
   */
  public native long split(
      long shuffleWriterId, int numRows, long handler) throws IOException;

  /**
   * Write the data remained in the buffers hold by native shuffle writer to each partition's
   * temporary file. And stop processing splitting
   *
   * @param shuffleWriterId shuffle writer instance id
   * @return SplitResult
   */
  public native SplitResult stop(long shuffleWriterId) throws IOException;

  /**
   * Release resources associated with designated shuffle writer instance.
   *
   * @param shuffleWriterId shuffle writer instance id
   */
  public native void close(long shuffleWriterId);
}
