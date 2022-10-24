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

import java.io.IOException;
import java.io.InputStream;

public class JniByteInputStreamImpl implements JniByteInputStream {
  private final InputStream in;
  private long bytesRead = 0L;

  public JniByteInputStreamImpl(InputStream in) {
    this.in = in;
  }

  @Override
  public long read(long destAddress, long maxSize) {
    int maxSize32 = Math.toIntExact(maxSize);
    byte[] tmp = new byte[maxSize32];
    try {
      int read = in.read(tmp);
      memCopyFromHeap(tmp, destAddress, read); // this conducts copy
      bytesRead += read;
      return read;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long tell() {
    return bytesRead;
  }

  public native void memCopyFromHeap(byte[] source, long destAddress, int size);

  @Override
  public void close() {
    try {
      in.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
