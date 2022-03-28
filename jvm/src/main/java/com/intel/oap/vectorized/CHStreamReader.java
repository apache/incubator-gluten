package com.intel.oap.vectorized;

import java.io.FileInputStream;
import java.io.InputStream;
import com.intel.oap.vectorized.CHColumnVector;

public class CHStreamReader implements AutoCloseable{
    private final long nativeShuffleReader;
    private final InputStream inputStream;

    private static native long createNativeShuffleReader (InputStream inputStream);

    public CHStreamReader(InputStream inputStream) {
        this.inputStream = inputStream;
        nativeShuffleReader = createNativeShuffleReader(this.inputStream);
    }

    public native long nativeNext(long nativeShuffleReader);

    public CHNativeBlock next() {
        long block = nativeNext(nativeShuffleReader);
        return new CHNativeBlock(block);
    }

    public native void nativeClose(long shuffleReader);

    @Override
    public void close() throws Exception {
        nativeClose(nativeShuffleReader);
    }

}
