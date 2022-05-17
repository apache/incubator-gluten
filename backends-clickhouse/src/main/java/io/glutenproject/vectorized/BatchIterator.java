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

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;

public class BatchIterator extends GeneralOutIterator {
  private native boolean nativeHasNext(long nativeHandle);
  private native byte[] nativeNext(long nativeHandle);
  private native long nativeCHNext(long nativeHandle);
  private native void nativeClose(long nativeHandle);
  private native MetricsObject nativeFetchMetrics(long nativeHandle);

  public BatchIterator(long instance_id) throws IOException {
    super(instance_id);
  }

  @Override
  public boolean hasNextInternal() throws IOException {
    return nativeHasNext(handle);
  }

  @Override
  public ColumnarBatch nextInternal() throws IOException {
    long block = nativeCHNext(handle);
    CHNativeBlock nativeBlock = new CHNativeBlock(block);
    int cols = nativeBlock.numColumns();
    ColumnVector[] columnVectors = new ColumnVector[cols];
    for (int i = 0; i < cols; i++) {
      columnVectors[i] = new CHColumnVector(DataType.fromDDL(nativeBlock.getTypeByPosition(i)), block, i);
    }
    return new ColumnarBatch(columnVectors, nativeBlock.numRows());
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
