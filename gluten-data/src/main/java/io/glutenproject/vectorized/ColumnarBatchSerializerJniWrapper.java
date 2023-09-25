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

import io.glutenproject.exec.ExecutionCtx;
import io.glutenproject.exec.ExecutionCtxAware;
import io.glutenproject.exec.ExecutionCtxs;
import io.glutenproject.init.JniInitialized;

public class ColumnarBatchSerializerJniWrapper extends JniInitialized implements ExecutionCtxAware {
  private final ExecutionCtx ctx;

  private ColumnarBatchSerializerJniWrapper(ExecutionCtx ctx) {
    this.ctx = ctx;
  }

  public static ColumnarBatchSerializerJniWrapper create() {
    return new ColumnarBatchSerializerJniWrapper(ExecutionCtxs.contextInstance());
  }

  public static ColumnarBatchSerializerJniWrapper forCtx(ExecutionCtx ctx) {
    return new ColumnarBatchSerializerJniWrapper(ctx);
  }

  @Override
  public long ctxHandle() {
    return ctx.getHandle();
  }

  public native ColumnarBatchSerializeResult serialize(long[] handles, long memoryManagerHandle);

  // Return the native ColumnarBatchSerializer handle
  public native long init(long cSchema, long memoryManagerHandle);

  public native long deserialize(long serializerHandle, byte[] data);

  public native void close(long serializerHandle);
}
