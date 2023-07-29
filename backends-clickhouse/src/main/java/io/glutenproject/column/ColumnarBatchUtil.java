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

package io.glutenproject.column;

import io.glutenproject.vectorized.CHNativeBlock;

import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.OutputStream;

public class ColumnarBatchUtil {

  private ColumnarBatchUtil() {}

  public static ColumnarBatch disposeBatch(ColumnarBatch cb) {
    if (cb != null) {
      if (cb.numCols() > 0) {
        CHNativeBlock block = CHNativeBlock.fromColumnarBatch(cb);
        block.close();
      }
      cb.close();
    }
    return null;
  }

  public static ColumnarBatch cloneBatch(ColumnarBatch cb) {
    long clonedAddress = cloneBatch(CHNativeBlock.fromColumnarBatchToAddress(cb));
    return CHNativeBlock.toColumnarBatch(clonedAddress);
  }

  public static void writeBatch(ColumnarBatch cb, OutputStream os, byte[] buffer, int bufferSize) {
    writeBatch(CHNativeBlock.fromColumnarBatchToAddress(cb), os, buffer, bufferSize);
  }

  public static void safeDisposeBatch(ColumnarBatch cb) {
    disposeBatch(CHNativeBlock.fromColumnarBatchToAddress(cb));
  }

  private static native long cloneBatch(long blockAddress);

  private static native void disposeBatch(long blockAddress);

  private static native void writeBatch(
      long blockAddress, OutputStream os, byte[] buffer, int bufferSize);
}
