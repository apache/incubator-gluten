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

import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class GeneralOutIterator implements AutoCloseable, Serializable {
  protected final long handle;
  protected final AtomicBoolean closed = new AtomicBoolean(false);
  protected final transient List<Attribute> outAttrs;

  public GeneralOutIterator(long handle, List<Attribute> outAttrs) {
    this.handle = handle;
    this.outAttrs = outAttrs;
  }

  private static StructType fromAttributes(List<Attribute> attributes) {
    // a => StructField(a.name, a.dataType, a.nullable, a.metadata))
    return new StructType(
        attributes.stream().map(attribute -> new StructField(attribute.name(),
            attribute.dataType(), attribute.nullable(), attribute.metadata())
        ).toArray(StructField[]::new));
  }

  public final boolean hasNext() throws Exception {
    return hasNextInternal();
  }

  public final ColumnarBatch next() throws Exception {
    return nextInternal();
  }

  public final Metrics getMetrics() throws Exception {
    return getMetricsInternal();
  }

  @Override
  public final void close() {
    if (closed.compareAndSet(false, true)) {
      closeInternal();
    }
  }


  public long getHandle() {
    return handle;
  }

  protected abstract void closeInternal();

  protected abstract boolean hasNextInternal() throws Exception;

  protected abstract ColumnarBatch nextInternal() throws Exception;

  protected abstract Metrics getMetricsInternal() throws Exception;

}
