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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class BlockOutputStream implements Closeable {
  private final long instance;
  private final OutputStream outputStream;

  private SQLMetric dataSize;

  private boolean isClosed = false;

  public BlockOutputStream(OutputStream outputStream, byte[] buffer, SQLMetric dataSize) {
    this.outputStream = outputStream;
    this.instance = nativeCreate(outputStream, buffer);
    this.dataSize = dataSize;
  }

  private native long nativeCreate(OutputStream outputStream, byte[] buffer);

  private native long nativeClose(long instance);

  private native void nativeWrite(long instance, long block);

  private native void nativeFlush(long instance);

  public void write(ColumnarBatch cb) {
    CHNativeBlock.fromColumnarBatch(cb).ifPresent(block -> {
      dataSize.add(block.totalBytes());
      nativeWrite(instance, block.blockAddress());
    });
  }

  public void flush() throws IOException {
    nativeFlush(instance);
    this.outputStream.flush();
  }

  @Override
  public void close() throws IOException {
    if (!isClosed) {
      nativeClose(instance);
      this.outputStream.flush();
      this.outputStream.close();
      isClosed = true;
    }
  }

  @Override
  protected void finalize() throws Throwable {
    close();
  }
}
