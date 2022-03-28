package com.intel.oap.vectorized;

import sun.misc.Unsafe;

import java.io.IOException;
import java.io.InputStream;

public class CHInputStreamWrapper {
    private final InputStream inputStream;

    public CHInputStreamWrapper(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public int read(long buffer, int size) throws IOException {
//        byte[] arr = new byte[size];
//        int count = inputStream.read(arr);
//        System.out.println("read bytes:"+count);
//        Unsafe unsafe = Unsafe.getUnsafe();
//        unsafe.copyMemory(arr, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, buffer, count);
        return size;
    }
}
