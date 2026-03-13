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
package org.apache.gluten.vectorized;

import org.apache.gluten.exception.GlutenException;

import org.apache.spark.shuffle.sort.FileSegmentsInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * This implementation is targeted to optimize against Gluten's {@link
 * org.apache.spark.shuffle.sort.FileSegmentsManagedBuffer} to make sure shuffle data is shared over
 * JNI without unnecessary copy.
 */
public class LowCopyFileSegmentsJniByteInputStream implements JniByteInputStream {
  private final FileSegmentsInputStream fsin;
  private long bytesRead = 0L;
  private long left;

  public LowCopyFileSegmentsJniByteInputStream(InputStream in) {
    final InputStream unwrapped = JniByteInputStreams.unwrapSparkInputStream(in);
    this.fsin = (FileSegmentsInputStream) unwrapped;
    left = this.fsin.remainingBytes();
  }

  public static boolean isSupported(InputStream in) {
    return in instanceof FileSegmentsInputStream;
  }

  @Override
  public long read(long destAddress, long maxSize) {
    long bytesToRead = Math.min(left, maxSize);
    if (bytesToRead == 0) {
      return 0;
    }
    try {
      long bytes = fsin.read(destAddress, bytesToRead);
      if (bytes == 0) {
        return 0;
      }
      bytesRead += bytes;
      left -= bytes;
      return bytes;
    } catch (IOException e) {
      throw new GlutenException(e);
    }
  }

  @Override
  public long tell() {
    return bytesRead;
  }

  @Override
  public void close() {
    try {
      fsin.close();
    } catch (IOException e) {
      throw new GlutenException(e);
    }
  }
}
