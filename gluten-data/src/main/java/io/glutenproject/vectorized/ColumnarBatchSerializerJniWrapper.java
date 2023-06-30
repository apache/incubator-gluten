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

import io.glutenproject.init.JniInitialized;

public class ColumnarBatchSerializerJniWrapper extends JniInitialized {

  public static final ColumnarBatchSerializerJniWrapper INSTANCE =
      new ColumnarBatchSerializerJniWrapper();

  private ColumnarBatchSerializerJniWrapper()  {}

  public native ColumnarBatchSerializeResult serialize(long[] handles, long allocId);

  // Return the native ColumnarBatchSerializer handle
  public native long init(long cSchema, long allocId);

  public native long deserialize(long handle, byte[] data);

  public native void close(long handle);

  // Record the exists batch twice
  public native long insertBatch(long handle);

  public native void closeBatches(long[] handles);
}
