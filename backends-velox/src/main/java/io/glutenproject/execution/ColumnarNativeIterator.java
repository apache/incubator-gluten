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

package io.glutenproject.execution;

import io.glutenproject.expression.ArrowConverterUtils;
import org.apache.arrow.dataset.jni.UnsafeRecordBatchSerializer;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Iterator;

public class ColumnarNativeIterator extends AbstractColumnarNativeIterator<long[]> {

  public ColumnarNativeIterator(Iterator<ColumnarBatch> delegated) {
    super(delegated);
  }

  @Override
  public long[] next() {
    ColumnarBatch dep_cb = nextBatch;
    if (dep_cb.numRows() > 0) {
      ArrowRecordBatch dep_rb = ArrowConverterUtils.createArrowRecordBatch(dep_cb);
      return serialize(dep_rb);
    } else {
      throw new IllegalStateException();
    }
  }

  private byte[] serialize(ArrowRecordBatch batch) {
    return UnsafeRecordBatchSerializer.serializeUnsafe(batch);
  }
}
