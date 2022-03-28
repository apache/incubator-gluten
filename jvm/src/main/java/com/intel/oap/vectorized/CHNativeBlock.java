package com.intel.oap.vectorized;

public class CHNativeBlock {
    private final long blockAddress;

    public CHNativeBlock(long blockAddress) {
        this.blockAddress = blockAddress;
    }

    private native int nativeNumRows(long blockAddress);

    public int numRows() {
        return nativeNumRows(blockAddress);
    };

    private native int nativeNumColumns(long blockAddress);

    public int numColumns() {
        return nativeNumColumns(blockAddress);
    }

    private native String nativeColumnType(long blockAddress, int position);

    public String getTypeByPosition(int position) {
        return nativeColumnType(blockAddress, position);
    }

    private native long nativeTotalBytes(long blockAddress);

    public long totalBytes() {
        return nativeTotalBytes(blockAddress);
    }
}
