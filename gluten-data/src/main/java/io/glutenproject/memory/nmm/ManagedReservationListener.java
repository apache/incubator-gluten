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
package io.glutenproject.memory.nmm;

import io.glutenproject.memory.MemoryUsage;
import io.glutenproject.memory.memtarget.MemoryTarget;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Reserve Spark managed memory. */
public class ManagedReservationListener implements ReservationListener {

  private static final Logger LOG = LoggerFactory.getLogger(ManagedReservationListener.class);

  private MemoryTarget target;
  private final MemoryUsage sharedUsage; // shared task metrics
  private volatile boolean open = true;

  public ManagedReservationListener(MemoryTarget target, MemoryUsage sharedUsage) {
    this.target = target;
    this.sharedUsage = sharedUsage;
  }

  @Override
  public long reserve(long size) {
    synchronized (this) {
      try {
        return target.borrow(size);
      } catch (Exception e) {
        LOG.error("Error reserving memory from target", e);
        throw e;
      }
    }
  }

  @Override
  public long unreserve(long size) {
    synchronized (this) {
      if (!open) {
        return 0L;
      }
      target.repay(size);
      sharedUsage.inc(-size);
      return size;
    }
  }

  @Override
  public long getUsedBytes() {
    return target.stats().current;
  }

  @Override
  public void inactivate() {
    synchronized (this) {
      target = null; // make it gc reachable
      open = false;
    }
  }

  public static class OutOfMemoryException extends RuntimeException {
    public OutOfMemoryException(String message) {
      super(message);
    }
  }
}
