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

public class CHSplitResult {
    private final long totalComputePidTime;
    private final long totalWriteTime;
    private final long totalEvictTime;
    private final long totalCompressTime; // overlaps with totalEvictTime and totalWriteTime
    private final long totalBytesWritten;
    private final long totalBytesEvicted;
    private final long[] partitionLengths;
    private final long[] rawPartitionLengths;
    private final long splitTime;
    private final long diskWriteTime;
    private final long serializationTime;
    private final long totalRows;
    private final long totalBatches;
    private final long wallTime;

    public CHSplitResult(long totalComputePidTime,
                         long totalWriteTime,
                         long totalEvictTime,
                         long totalCompressTime,
                         long totalBytesWritten,
                         long totalBytesEvicted,
                         long[] partitionLengths,
                         long[] rawPartitionLengths,
                         long splitTime,
                         long diskWriteTime,
                         long serializationTime,
                         long totalRows,
                         long totalBatches,
                         long wallTime) {
        this.totalComputePidTime = totalComputePidTime;
        this.totalWriteTime = totalWriteTime;
        this.totalEvictTime = totalEvictTime;
        this.totalCompressTime = totalCompressTime;
        this.totalBytesWritten = totalBytesWritten;
        this.totalBytesEvicted = totalBytesEvicted;
        this.partitionLengths = partitionLengths;
        this.rawPartitionLengths = rawPartitionLengths;
        this.splitTime = splitTime;
        this.diskWriteTime = diskWriteTime;
        this.serializationTime = serializationTime;
        this.totalRows = totalRows;
        this.totalBatches = totalBatches;
        this.wallTime = wallTime;
    }

    public long getTotalComputePidTime() {
        return totalComputePidTime;
    }

    public long getTotalWriteTime() {
        return totalWriteTime;
    }

    public long getTotalSpillTime() {
        return totalEvictTime;
    }

    public long getTotalCompressTime() {
        return totalCompressTime;
    }

    public long getTotalBytesWritten() {
        return totalBytesWritten;
    }

    public long getTotalBytesSpilled() {
        return totalBytesEvicted;
    }

    public long getTotalPushTime() {
        return totalEvictTime;
    }

    public long[] getPartitionLengths() {
        return partitionLengths;
    }

    public long[] getRawPartitionLengths() {
        return rawPartitionLengths;
    }

    public long getSplitTime() {
        return splitTime;
    }

    public long getDiskWriteTime() {
        return diskWriteTime;
    }

    public long getSerializationTime() {
        return serializationTime;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public long getTotalBatches() {
        return totalBatches;
    }

    public long getWallTime() {
        return wallTime;
    }
}
