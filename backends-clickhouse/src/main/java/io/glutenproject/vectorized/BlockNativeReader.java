package io.glutenproject.vectorized;

import java.io.InputStream;

public class BlockNativeReader {
    private final long instance;

    public BlockNativeReader(InputStream in) {
        instance = nativeCreate(in);
    }

    private native long nativeCreate(InputStream in);

    private native void nativeClose(long instance);

    private native boolean nativeHasNext(long instance);

    private native long nativeNext(long instance);

    public boolean hasNext() {
        return nativeHasNext(instance);
    }

    public long next() {
        return nativeNext(instance);
    }

    @Override
    protected void finalize() throws Throwable {
        nativeClose(instance);
    }
}
