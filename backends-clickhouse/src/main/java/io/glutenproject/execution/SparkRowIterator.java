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

package io.glutenproject.execution;

import java.util.Iterator;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SparkRowIterator implements Iterator<byte[]> {
    private final scala.collection.Iterator<byte[]> delegated;
    private final int maxBufSize = 4096;
    private byte[] lastRowBuf;

    public SparkRowIterator(scala.collection.Iterator<byte[]> delegated) {
        this.delegated = delegated;
        lastRowBuf = null;
    }

    @Override
    public boolean hasNext() {
        return lastRowBuf != null || delegated.hasNext();
    }

    @Override
    public byte[] next() {
        return delegated.next();
    }

    public ByteBuffer nextBatch() {
        ByteBuffer buf = ByteBuffer.allocateDirect(maxBufSize);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        if (lastRowBuf != null) {
            buf.putInt(lastRowBuf.length);
            buf.put(lastRowBuf);
            lastRowBuf = null;
        }
        while (buf.remaining() > 4) {
            if (!delegated.hasNext()) {
                lastRowBuf = null;
                break;
            }
            lastRowBuf = delegated.next();
            if (buf.remaining() < lastRowBuf.length + 8) {
                break;
            }
            else {
                buf.putInt(lastRowBuf.length);
                buf.put(lastRowBuf);
                lastRowBuf = null;
            }
        }
        // make the end flag
        buf.putInt(-1);
        return buf;
    }
}
