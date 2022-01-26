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

package com.intel.oap.row;

import com.intel.oap.vectorized.MetricsObject;

import java.io.IOException;

public class RowIterator {
  private native boolean nativeHasNext(long nativeHandler);
  private native SparkRowInfo nativeNext(long nativeHandler);
  private native void nativeClose(long nativeHandler);
  private native MetricsObject nativeFetchMetrics(long nativeHandler);

  private long nativeHandler = 0L;
  private boolean closed = false;

  public RowIterator(long nativeHandler) throws IOException {
      this.nativeHandler = nativeHandler;
  }

  public boolean hasNext() throws IOException {
      return nativeHasNext(nativeHandler);
  }

  public SparkRowInfo next() throws IOException {
    if (nativeHandler == 0) {
      return null;
    }
    return nativeNext(nativeHandler);
  }

  public MetricsObject getMetrics() throws IOException, ClassNotFoundException {
    if (nativeHandler == 0) {
      return null;
    }
    return nativeFetchMetrics(nativeHandler);
  }

  public void close() {
    if (!closed) {
      if (nativeHandler != 0L) {
          nativeClose(nativeHandler);
      }
      closed = true;
    }
  }
}
