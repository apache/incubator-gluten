package com.intel.oap.vectorized;

import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

public class CHCoalesceOperator implements Serializable, Closeable {
    private static final long serialVersionUID = -1L;
    private long nativeOperator;

    public CHCoalesceOperator(int bufSize) {
        this.nativeOperator = createNativeOperator(bufSize);
    }

    public native long createNativeOperator(int bufSize);

    public native boolean nativeIsFull(long nativeOperator);

    public boolean isFull() {
        return nativeIsFull(nativeOperator);
    }

    public native void nativeMergeBlock(long nativeOperator, long block);

    public void mergeBlock(ColumnarBatch block) {
        if (block.numCols() == 0) return;
        CHColumnVector col = (CHColumnVector) block.column(0);
        long blockAddress = col.getBlockAddress();
        nativeMergeBlock(nativeOperator, blockAddress);
        CHNativeBlock nativeBlock = new CHNativeBlock(blockAddress);
        nativeBlock.close();
    }

    public native long nativeRelease(long nativeOperator);

    public CHNativeBlock release() {
        long block = nativeRelease(nativeOperator);
        return new CHNativeBlock(block);
    }

    public native void nativeClose(long nativeOperator);

    @Override
    public void close() throws IOException {
        if (nativeOperator != 0) {
            nativeClose(nativeOperator);
            nativeOperator = 0;
        }
    }
}
