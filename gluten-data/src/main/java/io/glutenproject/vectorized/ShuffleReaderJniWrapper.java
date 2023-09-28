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

public class ShuffleReaderJniWrapper extends JniInitialized implements ExecutionCtxAware {
  private final ExecutionCtx ctx;

  private ShuffleReaderJniWrapper(ExecutionCtx ctx) {
    this.ctx = ctx;
  }

  public static ShuffleReaderJniWrapper create() {
    return new ShuffleReaderJniWrapper(ExecutionCtxs.contextInstance());
  }

  @Override
  public long ctxHandle() {
    return ctx.getHandle();
  }

  public native long make(
      long cSchema,
      long memoryManagerHandle,
      String compressionType,
      String compressionCodecBackend,
      String compressionMode);

  public native long readStream(long shuffleReaderHandle, JniByteInputStream jniIn);

  public native void populateMetrics(long shuffleReaderHandle, ShuffleReaderMetrics metrics);

  public native void close(long shuffleReaderHandle);
}
