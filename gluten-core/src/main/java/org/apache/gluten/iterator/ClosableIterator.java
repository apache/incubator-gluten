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
package org.apache.gluten.iterator;

import org.apache.gluten.exception.GlutenException;

import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.Serializable;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ClosableIterator
    implements AutoCloseable, Serializable, Iterator<ColumnarBatch> {
  protected final AtomicBoolean closed = new AtomicBoolean(false);

  public ClosableIterator() {}

  @Override
  public final boolean hasNext() {
    if (closed.get()) {
      throw new GlutenException("Iterator has been closed.");
    }
    try {
      return hasNext0();
    } catch (Exception e) {
      throw new GlutenException(e);
    }
  }

  @Override
  public final ColumnarBatch next() {
    if (closed.get()) {
      throw new GlutenException("Iterator has been closed.");
    }
    try {
      return next0();
    } catch (Exception e) {
      throw new GlutenException(e);
    }
  }

  @Override
  public final void close() {
    if (closed.compareAndSet(false, true)) {
      close0();
    }
  }

  protected abstract void close0();

  protected abstract boolean hasNext0() throws Exception;

  protected abstract ColumnarBatch next0() throws Exception;
}
