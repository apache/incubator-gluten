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
import io.glutenproject.exec.ExecutionCtx;
import io.glutenproject.exec.ExecutionCtxAware;
import io.glutenproject.metrics.IMetrics;

import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;

public class ColumnarBatchOutIterator extends GeneralOutIterator implements ExecutionCtxAware {
  private final ExecutionCtx ctx;
  private final long iterHandle;

  public ColumnarBatchOutIterator(ExecutionCtx ctx, long iterHandle) throws IOException {
    super();
    this.ctx = ctx;
    this.iterHandle = iterHandle;
  }

  @Override
  public long ctxHandle() {
    return ctx.getHandle();
  }

  @Override
  public String getId() {
    // Using native iterHandle as identifier
    return String.valueOf(iterHandle);
  }

  private native boolean nativeHasNext(long iterHandle);

  private native long nativeNext(long iterHandle);

  private native long nativeSpill(long iterHandle, long size);

  private native void nativeClose(long iterHandle);

  private native IMetrics nativeFetchMetrics(long iterHandle);

  @Override
  public boolean hasNextInternal() throws IOException {
    return nativeHasNext(iterHandle);
  }

  @Override
  public ColumnarBatch nextInternal() throws IOException {
    long batchHandle = nativeNext(iterHandle);
    if (batchHandle == -1L) {
      return null; // stream ended
    }
    return ColumnarBatches.create(ctx, batchHandle);
  }

  @Override
  public IMetrics getMetricsInternal() throws IOException, ClassNotFoundException {
    return nativeFetchMetrics(iterHandle);
  }

  public long spill(long size) {
    return nativeSpill(iterHandle, size);
  }

  @Override
  public void closeInternal() {
    nativeClose(iterHandle);
  }
}
