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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// For debugging purpose only
public class LoggingReservationListener implements ReservationListener {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoggingReservationListener.class);

  private final ReservationListener delegated;

  public LoggingReservationListener(ReservationListener delegated) {
    this.delegated = delegated;
  }

  @Override
  public long reserve(long size) {
    long before = getUsedBytes();
    long reserved = delegated.reserve(size);
    long after = getUsedBytes();
    LOGGER.info(String.format("Reservation: %d + %d(%d) = %d", before, reserved, size, after));
    return reserved;
  }

  @Override
  public long unreserve(long size) {
    long before = getUsedBytes();
    long unreserved = delegated.unreserve(size);
    long after = getUsedBytes();
    LOGGER.info(String.format("Unreservation: %d - %d(%d) = %d", before, unreserved, size, after));
    return unreserved;
  }

  @Override
  public long getUsedBytes() {
    return delegated.getUsedBytes();
  }
}
