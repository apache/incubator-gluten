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

import java.io.IOException;

import java.io.Serializable;

abstract public class AbstractBatchIterator implements AutoCloseable, Serializable {

  protected long nativeHandler = 0;
  protected boolean closed = false;

  public AbstractBatchIterator() throws IOException {}

  public AbstractBatchIterator(long instance_id) throws IOException {
    nativeHandler = instance_id;
  }

  public abstract boolean hasNextInternal() throws IOException;

  public boolean hasNext() throws IOException {
    return hasNextInternal();
  }

  public abstract <T> T nextInternal() throws IOException;

  public <T> T next() throws IOException {
    return nextInternal();
  }

  public abstract MetricsObject getMetricsInternal() throws IOException, ClassNotFoundException;

  public MetricsObject getMetrics() throws IOException, ClassNotFoundException {
    if (nativeHandler == 0) {
      return null;
    }
    return getMetricsInternal();
  }

  public abstract void closeInternal();

  @Override
  public void close() {
    closeInternal();
  }

  long getInstanceId() {
    return nativeHandler;
  }
}
