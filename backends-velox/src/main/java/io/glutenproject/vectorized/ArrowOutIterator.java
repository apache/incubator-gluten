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

import io.glutenproject.columnarbatch.GlutenColumnarBatches;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.List;

public class ArrowOutIterator extends GeneralOutIterator {

  public ArrowOutIterator(long instance_id, List<Attribute> outAttrs) throws IOException {
    super(instance_id, outAttrs);
  }

  private native boolean nativeHasNext(long nativeHandle);

  private native long nativeNext(long nativeHandle);

  private native long nativeCHNext(long nativeHandle);

  private native void nativeClose(long nativeHandle);

  private native Metrics nativeFetchMetrics(long nativeHandle);

  @Override
  public boolean hasNextInternal() throws IOException {
    return nativeHasNext(handle);
  }

  @Override
  public ColumnarBatch nextInternal() throws IOException {
    long batchHandle = nativeNext(handle);
    if (batchHandle == -1L) {
      return null; // stream ended
    }
    return GlutenColumnarBatches.create(batchHandle);
  }

  @Override
  public Metrics getMetricsInternal() throws IOException, ClassNotFoundException {
    return nativeFetchMetrics(handle);
  }

  @Override
  public void closeInternal() {
    nativeClose(handle);
  }
}
