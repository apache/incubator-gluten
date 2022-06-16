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

import java.io.Serializable;
import java.util.Iterator;

public class BlockSplitIterator implements Iterator<ColumnarBatch> {
    public static class IteratorOptions implements Serializable {
        private static final long serialVersionUID = -1L;
        private int partitionNum;
        private String name;
        private int bufferSize;
        private String expr;

        public int getPartitionNum() {
            return partitionNum;
        }

        public void setPartitionNum(int partitionNum) {
            this.partitionNum = partitionNum;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getBufferSize() {
            return bufferSize;
        }

        public void setBufferSize(int bufferSize) {
            this.bufferSize = bufferSize;
        }

        public String getExpr() {
            return expr;
        }

        public void setExpr(String expr) {
            this.expr = expr;
        }
    }

    private long instance = 0;

    public BlockSplitIterator(Iterator<Long> in, IteratorOptions options) {
        this.instance = nativeCreate(new IteratorWrapper(in), options.getName(), options.getExpr(),
                options.getPartitionNum(), options.getBufferSize());
    }

    private native long nativeCreate(IteratorWrapper in, String name, String expr, int partitionNum, int bufferSize);

    private native void nativeClose(long instance);

    private native boolean nativeHasNext(long instance);

    @Override
    public boolean hasNext() {
        return nativeHasNext(instance);
    }

    private native long nativeNext(long instance);

    @Override
    public ColumnarBatch next() {
        CHNativeBlock block = new CHNativeBlock(nativeNext(instance));
        return block.toColumnarBatch();
    }

    private native int nativeNextPartitionId(long instance);

    public int nextPartitionId() {
        return nativeNextPartitionId(instance);
    }

    @Override
    protected void finalize() throws Throwable {
        nativeClose(instance);
    }
}
