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

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.glutenproject.memory.GlutenMemoryConsumer;
import io.glutenproject.memory.TaskMemoryMetrics;

public class CHManagedReservationListener implements ReservationListener {

  private static final Logger LOG =
      LoggerFactory.getLogger(CHManagedReservationListener.class);

  private GlutenMemoryConsumer consumer;
  private TaskMemoryMetrics metrics;
  private volatile boolean open = true;

  private final AtomicLong currentMemory = new AtomicLong(0L);

  public CHManagedReservationListener(GlutenMemoryConsumer consumer,
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
      LOG.debug("reserve memory size from native: " + size);
      long granted = consumer.acquire(size);
      if (granted < size) {
        consumer.free(granted);
        throw new UnsupportedOperationException("Not enough spark off-heap execution memory. " +
            "Acquired: " + size + ", granted: " + granted + ". " +
            "Try tweaking config option spark.memory.offHeap.size to " +
            "get larger space to run this application. ");
      }
      currentMemory.addAndGet(size);
      metrics.inc(size);
    }
  }

  @Override
  public long reserve(long size) {
    synchronized (this) {
      if (!open) {
        return 0L;
      }
      LOG.debug("reserve memory (without exception) size from native: " + size);
      long granted = consumer.acquire(size);
      if (granted < size) {
        LOG.warn("Not enough spark off-heap execution memory. " +
            "Acquired: " + size + ", granted: " + granted + ". " +
            "Try tweaking config option spark.memory.offHeap.size to " +
            "get larger space to run this application. ");
      }
      currentMemory.addAndGet(granted);
      metrics.inc(size);
      return granted;
    }
  }

  @Override
  public long unreserve(long size) {
    synchronized (this) {
      if (!open) {
        return 0L;
      }
      long memoryToFree = size;
      if ((currentMemory.get() - size) < 0L) {
        LOG.warn(
            "The current used memory' " + currentMemory.get() +
                " will be less than 0 after free " + size
        );
        memoryToFree = currentMemory.get();
      }
      if (memoryToFree == 0L) return memoryToFree;
      LOG.debug("unreserve memory size from native: " + memoryToFree);
      consumer.free(memoryToFree);
      currentMemory.addAndGet(-memoryToFree);
      metrics.inc(-size);
      return memoryToFree;
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
    return currentMemory.get();
  }
}
