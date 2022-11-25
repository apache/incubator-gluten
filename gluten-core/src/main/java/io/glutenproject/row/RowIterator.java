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

package io.glutenproject.row;

import io.glutenproject.vectorized.Metrics;

import java.io.IOException;

/**
 * This is a built-in native implementation of {@link BaseRowIterator}. This is a part of the
 * provided {@link io.glutenproject.vectorized.NativeExpressionEvaluator} implementation as
 * JNI facility for wiring with native codes.
 */
// FIXME can we rename this by adding a "native" prefix to make it clear the iterator is
//   implemented in native?
public class RowIterator implements BaseRowIterator {
  private final long nativeHandler;
  private boolean closed = false;

  public RowIterator(long nativeHandler) {
    this.nativeHandler = nativeHandler;
  }

  private native boolean nativeHasNext(long nativeHandler);

  private native SparkRowInfo nativeNext(long nativeHandler);

  private native void nativeClose(long nativeHandler);

  private native Metrics nativeFetchMetrics(long nativeHandler);

  @Override
  public boolean hasNext() throws IOException {
    return nativeHasNext(nativeHandler);
  }

  @Override
  public SparkRowInfo next() throws IOException {
    if (nativeHandler == 0) {
      return null;
    }
    return nativeNext(nativeHandler);
  }

  public Metrics getMetrics() {
    if (nativeHandler == 0) {
      return null;
    }
    return nativeFetchMetrics(nativeHandler);
  }

  @Override
  public void close() {
    if (!closed) {
      if (nativeHandler != 0L) {
        nativeClose(nativeHandler);
      }
      closed = true;
    }
  }
}
