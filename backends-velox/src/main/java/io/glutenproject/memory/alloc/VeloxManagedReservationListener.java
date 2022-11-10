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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.glutenproject.memory.GlutenMemoryConsumer;
import io.glutenproject.memory.TaskMemoryMetrics;

/**
 * Reserve Spark managed memory.
 */
public class VeloxManagedReservationListener implements ReservationListener {

  private static final Logger LOG =
      LoggerFactory.getLogger(VeloxManagedReservationListener.class);

  private GlutenMemoryConsumer consumer;
  private TaskMemoryMetrics metrics;
  private volatile boolean open = true;

  public VeloxManagedReservationListener(GlutenMemoryConsumer consumer,
                                         TaskMemoryMetrics metrics) {
    this.consumer = consumer;
    this.metrics = metrics;
  }

  @Override
  public void reserveOrThrow(long size) {
    synchronized (this) {
      if (!open) {
        return;
      }
      long granted = consumer.acquire(size);
      if (granted < size) {
        consumer.free(granted);
        throw new UnsupportedOperationException("Not enough spark off-heap execution memory. " +
            "Acquired: " + size + ", granted: " + granted + ". " +
            "Try tweaking config option spark.memory.offHeap.size to " +
            "get larger space to run this application. ");
      }
      metrics.inc(size);
    }
  }

  @Override
  public long reserve(long size) {
    synchronized (this) {
      if (!open) {
        return 0L;
      }
      long granted = consumer.acquire(size);
      if (granted < size) {
        LOG.warn("Not enough spark off-heap execution memory. " +
            "Acquired: " + size + ", granted: " + granted + ". " +
            "Try tweaking config option spark.memory.offHeap.size to " +
            "get larger space to run this application. ");
      }
      metrics.inc(granted);
      return granted;
    }
  }

  @Override
  public long unreserve(long size) {
    synchronized (this) {
      if (!open) {
        return 0L;
      }
      consumer.free(size);
      metrics.inc(-size);
      return size;
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
