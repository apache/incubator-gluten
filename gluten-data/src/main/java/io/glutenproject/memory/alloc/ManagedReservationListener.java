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

import io.glutenproject.memory.TaskMemoryMetrics;
import io.glutenproject.memory.memtarget.MemoryTarget;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

/** Reserve Spark managed memory. */
public class ManagedReservationListener implements ReservationListener {

  private static final Logger LOG = LoggerFactory.getLogger(ManagedReservationListener.class);

  private MemoryTarget target;
  private final TaskMemoryMetrics metrics;
  private volatile boolean open = true;
  private volatile long reserved = 0L;

  public ManagedReservationListener(MemoryTarget target, TaskMemoryMetrics metrics) {
    this.target = target;
    this.metrics = metrics;
  }

  private long reserve0(long size, Consumer<String> onOom) {
    synchronized (this) {
      if (!open) {
        return 0L;
      }
      long granted = target.borrow(size);
      reserved += granted;
      metrics.inc(granted);
      if (granted < size) {
        if (granted != 0L) {
          target.repay(granted);
        }
        onOom.accept(
            "Not enough spark off-heap execution memory. "
                + "Acquired: "
                + size
                + ", granted: "
                + granted
                + ". "
                + "Try tweaking config option spark.memory.offHeap.size to "
                + "get larger space to run this application. ");
      }
      return granted;
    }
  }

  @Override
  public void reserveOrThrow(long size) {
    reserve0(
        size,
        s -> {
          throw new UnsupportedOperationException(s);
        });
  }

  @Override
  public long reserve(long size) {
    return reserve0(size, LOG::warn);
  }

  @Override
  public long unreserve(long size) {
    synchronized (this) {
      if (!open) {
        return 0L;
      }
      target.repay(size);
      reserved -= size;
      metrics.inc(-size);
      return size;
    }
  }

  @Override
  public void inactivate() {
    synchronized (this) {
      target = null; // make it gc reachable
      open = false;
    }
  }
}
