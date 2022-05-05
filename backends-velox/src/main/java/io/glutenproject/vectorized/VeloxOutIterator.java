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

import io.glutenproject.utils.ArrowAbiUtil;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;

public class VeloxOutIterator extends GeneralOutIterator<ColumnarBatch> {

  private native boolean nativeHasNext(long nativeHandle);
  private native boolean nativeNext(long nativeHandle, long cSchema, long cArray);
  private native long nativeCHNext(long nativeHandle);
  private native void nativeClose(long nativeHandle);
  private native MetricsObject nativeFetchMetrics(long nativeHandle);

  public VeloxOutIterator(long instance_id) throws IOException {
    super(instance_id);
  }

  @Override
  public boolean hasNextInternal() throws IOException {
    return nativeHasNext(handle);
  }

  @Override
  public ColumnarBatch nextInternal() throws IOException {
    final BufferAllocator allocator = SparkMemoryUtils.contextAllocator();
    final ArrowArray cArray = ArrowArray.allocateNew(allocator);
    final ArrowSchema cSchema = ArrowSchema.allocateNew(allocator);
    if (!nativeNext(handle, cSchema.memoryAddress(), cArray.memoryAddress())) {
      return null; // stream ended
    }
    return ArrowAbiUtil.importToSparkColumnarBatch(allocator, cSchema, cArray);
  }

  @Override
  public MetricsObject getMetricsInternal() throws IOException, ClassNotFoundException {
    return nativeFetchMetrics(handle);
  }

  @Override
  public void closeInternal() {
    nativeClose(handle);
  }
}
