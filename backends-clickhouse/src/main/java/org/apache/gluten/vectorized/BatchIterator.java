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

import org.apache.gluten.iterator.ClosableIterator;
import org.apache.gluten.metrics.NativeMetrics;

import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.concurrent.atomic.AtomicBoolean;

public class BatchIterator extends ClosableIterator {
  private final long handle;
  private final AtomicBoolean cancelled = new AtomicBoolean(false);

  public BatchIterator(long handle) {
    super();
    this.handle = handle;
  }

  private native boolean nativeHasNext(long nativeHandle);

  private native long nativeCHNext(long nativeHandle);

  private native void nativeClose(long nativeHandle);

  private native void nativeCancel(long nativeHandle);

  private native String nativeFetchMetrics(long nativeHandle);

  @Override
  public boolean hasNext0() {
    return nativeHasNext(handle);
  }

  @Override
  public ColumnarBatch next0() {
    long block = nativeCHNext(handle);
    CHNativeBlock nativeBlock = new CHNativeBlock(block);
    return nativeBlock.toColumnarBatch();
  }

  public NativeMetrics getMetrics() {
    return new NativeMetrics(nativeFetchMetrics(handle));
  }

  @Override
  public void close0() {
    nativeClose(handle);
  }

  // Used to cancel native pipeline execution when spark task is killed
  public final void cancel() {
    if (cancelled.compareAndSet(false, true)) {
      nativeCancel(handle);
    }
  }
}
