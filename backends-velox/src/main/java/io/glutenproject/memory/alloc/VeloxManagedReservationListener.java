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

package io.glutenproject.memory.alloc;

import io.glutenproject.memory.GlutenMemoryConsumer;
import io.glutenproject.memory.TaskMemoryMetrics;

/**
 * Reserve Spark managed memory.
 */
public class VeloxManagedReservationListener implements ReservationListener {

  private GlutenMemoryConsumer consumer;
  private TaskMemoryMetrics metrics;
  private volatile boolean open = true;

  public VeloxManagedReservationListener(GlutenMemoryConsumer consumer,
                                         TaskMemoryMetrics metrics) {
    this.consumer = consumer;
    this.metrics = metrics;
  }

  @Override
  public void reserve(long size) {
    synchronized (this) {
      if (!open) {
        return;
      }
      consumer.acquire(size);
      metrics.inc(size);
    }
  }

  @Override
  public void unreserve(long size) {
    synchronized (this) {
      if (!open) {
        return;
      }
      consumer.free(size);
      metrics.inc(-size);
    }
  }

  @Override
  public void inactivate() {
    synchronized (this) {
      consumer = null; // make it gc reachable
      open = false;
    }
  }

  @Override
  public long currentMemory() {
    return consumer.getUsed();
  }
}
