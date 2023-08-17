package io.glutenproject.vectorized;

import io.glutenproject.vectorized.SplitResult;

public class CHSplitResult extends SplitResult {
    private final long splitTime;
    private final long diskWriteTime;
    private final long serializationTime;

    public CHSplitResult(long totalComputePidTime, long totalWriteTime, long totalEvictTime, long totalCompressTime, long totalBytesWritten, long totalBytesEvicted, long[] partitionLengths, long[] rawPartitionLengths, long splitTime, long diskWriteTime, long serializationTime) {
        super(totalComputePidTime, totalWriteTime, totalEvictTime, totalCompressTime, totalBytesWritten, totalBytesEvicted, partitionLengths, rawPartitionLengths);
        this.splitTime = splitTime;
        this.diskWriteTime = diskWriteTime;
        this.serializationTime = serializationTime;
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
}
