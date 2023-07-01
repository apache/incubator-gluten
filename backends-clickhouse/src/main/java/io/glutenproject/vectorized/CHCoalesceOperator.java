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
