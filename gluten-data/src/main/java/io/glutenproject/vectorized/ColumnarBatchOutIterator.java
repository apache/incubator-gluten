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

import io.glutenproject.columnarbatch.ColumnarBatches;
import io.glutenproject.metrics.IMetrics;

import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;

public class ColumnarBatchOutIterator extends GeneralOutIterator {
  private final long executionCtxHandle;
  private final long iterHandle;

  public ColumnarBatchOutIterator(long executionCtxHandle, long iterHandle) throws IOException {
    super();
    this.executionCtxHandle = executionCtxHandle;
    this.iterHandle = iterHandle;
  }

  @Override
  public String getId() {
    // Using native iterHandle as identifier
    return String.valueOf(iterHandle);
  }

  private native boolean nativeHasNext(long executionCtxHandle, long iterHandle);

  private native long nativeNext(long executionCtxHandle, long iterHandle);

  private native long nativeSpill(long executionCtxHandle, long iterHandle, long size);

  private native void nativeClose(long executionCtxHandle, long iterHandle);

  private native IMetrics nativeFetchMetrics(long executionCtxHandle, long iterHandle);

  @Override
  public boolean hasNextInternal() throws IOException {
    return nativeHasNext(executionCtxHandle, iterHandle);
  }

  @Override
  public ColumnarBatch nextInternal() throws IOException {
    long batchHandle = nativeNext(executionCtxHandle, iterHandle);
    if (batchHandle == -1L) {
      return null; // stream ended
    }
    return ColumnarBatches.create(executionCtxHandle, batchHandle);
  }

  @Override
  public IMetrics getMetricsInternal() throws IOException, ClassNotFoundException {
    return nativeFetchMetrics(executionCtxHandle, iterHandle);
  }

  public long spill(long size) {
    return nativeSpill(executionCtxHandle, iterHandle, size);
  }

  @Override
  public void closeInternal() {
    nativeClose(executionCtxHandle, iterHandle);
  }
}
