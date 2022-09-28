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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SparkRowIterator implements Iterator<byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(SparkRowIterator.class);
    private final scala.collection.Iterator<byte[]> delegated;
    private byte[] lastArray;

    public SparkRowIterator(scala.collection.Iterator<byte[]> delegated) {
        this.delegated = delegated;
        lastArray = null;
    }

    @Override
    public boolean hasNext() {
        return lastArray != null || delegated.hasNext();
    }

    @Override
    public byte[] next() {
        return delegated.next();
    }
    public ByteBuffer nextBuf() {
        int max_size = 4096;
        ByteBuffer buf = ByteBuffer.allocateDirect(max_size);
        buf.order(ByteOrder.LITTLE_ENDIAN);
        int size = 0;
        if (lastArray != null) {
            buf.putInt(lastArray.length);
            buf.put(lastArray);
            size += lastArray.length + 4;
            lastArray = null;
        }
        while (size + 4 < max_size) {
            if (!delegated.hasNext()) {
                lastArray = null;
                break;
            }
            lastArray = delegated.next();
            if (size + 4 + lastArray.length > max_size) {
                break;
            }
            else {
                size += lastArray.length + 4;
                buf.putInt(lastArray.length);
                buf.put(lastArray);
                lastArray = null;
            }
        }
        // make the end flag
        buf.putInt(-1);
        return buf;
    }
}
