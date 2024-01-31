package io.glutenproject.vectorized;

import io.glutenproject.exec.Runtime;
import io.glutenproject.exec.RuntimeAware;
import io.glutenproject.exec.Runtimes;

public class VanillaColumnarToNativeColumnarJniWrapper implements RuntimeAware {
    private final Runtime runtime;

    private VanillaColumnarToNativeColumnarJniWrapper(Runtime runtime) {
        this.runtime = runtime;
    }

    public static VanillaColumnarToNativeColumnarJniWrapper create() {
        return new VanillaColumnarToNativeColumnarJniWrapper(Runtimes.contextInstance());
    }

    @Override
    public long handle() {
        return runtime.getHandle();
    }

    public native long init(long cSchema, long memoryManagerHandle);

    public native long nativeConvertVanillaColumnarToColumnar(
            long c2cHandle, long bufferAddress);

    public native void close(long c2cHandle);
}
