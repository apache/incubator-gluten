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
package io.glutenproject.columnarbatch;

import io.glutenproject.exec.ExecutionCtx;
import io.glutenproject.exec.ExecutionCtxAware;
import io.glutenproject.exec.ExecutionCtxs;
import io.glutenproject.init.JniInitialized;

public class ColumnarBatchJniWrapper extends JniInitialized implements ExecutionCtxAware {
  private final ExecutionCtx ctx;

  private ColumnarBatchJniWrapper(ExecutionCtx ctx) {
    this.ctx = ctx;
  }

  public static ColumnarBatchJniWrapper create() {
    return new ColumnarBatchJniWrapper(ExecutionCtxs.contextInstance());
  }

  public static ColumnarBatchJniWrapper forCtx(ExecutionCtx ctx) {
    return new ColumnarBatchJniWrapper(ctx);
  }

  public native long createWithArrowArray(long cSchema, long cArray);

  public native String getType(long batchHandle);

  public native long numColumns(long batchHandle);

  public native long numRows(long batchHandle);

  public native long numBytes(long batchHandle);

  public native long compose(long[] batches);

  public native void exportToArrow(long batch, long cSchema, long cArray);

  public native long select(
      long nativeMemoryManagerHandle, // why a mm is needed here?
      long batch,
      int[] columnIndices);

  public native void close(long batch);

  @Override
  public long ctxHandle() {
    return ctx.getHandle();
  }
}
