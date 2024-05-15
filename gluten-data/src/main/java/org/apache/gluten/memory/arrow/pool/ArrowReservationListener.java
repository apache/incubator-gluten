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
package org.apache.gluten.memory.arrow.pool;

import org.apache.gluten.memory.SimpleMemoryUsageRecorder;

public class ArrowReservationListener implements org.apache.arrow.dataset.jni.ReservationListener {
  private final SimpleMemoryUsageRecorder sharedUsage; // shared task metrics

  public ArrowReservationListener(SimpleMemoryUsageRecorder recorder) {
    this.sharedUsage = recorder;
  }

  @Override
  public void reserve(long size) {
    synchronized (this) {
      sharedUsage.inc(size);
    }
  }

  @Override
  public void unreserve(long size) {
    synchronized (this) {
      sharedUsage.inc(-size);
    }
  }
}
