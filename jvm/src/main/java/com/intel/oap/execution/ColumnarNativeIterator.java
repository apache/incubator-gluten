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

package com.intel.oap.execution;

import com.intel.oap.expression.ConverterUtils;
import org.apache.arrow.dataset.jni.NativeSerializedRecordBatchIterator;
import org.apache.arrow.dataset.jni.UnsafeRecordBatchSerializer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Iterator;

public class ColumnarNativeIterator implements NativeSerializedRecordBatchIterator {
  private final Iterator<ColumnarBatch> delegated;
  private ColumnarBatch nextBatch = null;

  public ColumnarNativeIterator(Iterator<ColumnarBatch> delegated) {
    this.delegated = delegated;
  }

  @Override
  public boolean hasNext() {
    while (delegated.hasNext()) {
      nextBatch = delegated.next();
      if (nextBatch.numRows() > 0) {
        return true;
      }
    }
    return false;
  }

  @Override
  public byte[] next() {
    ColumnarBatch dep_cb = nextBatch;
    if (dep_cb.numRows() > 0) {
      ArrowRecordBatch dep_rb = ConverterUtils.createArrowRecordBatch(dep_cb);
      return serialize(dep_rb);
    } else {
      throw new IllegalStateException();
    }
  }

  private byte[] serialize(ArrowRecordBatch batch) {
    return UnsafeRecordBatchSerializer.serializeUnsafe(batch);
  }

  @Override
  public void close() throws Exception {

  }
}
