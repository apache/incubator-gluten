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

public interface ReservationListener {
  ReservationListener NOOP =
      new ReservationListener() {
        @Override
        public void reserveOrThrow(long size) {}

        @Override
        public long reserve(long size) {
          return 0L;
        }

        @Override
        public long unreserve(long size) {
          return 0L;
        }

        @Override
        public void inactivate() {}
      };

  long reserve(long size);

  void reserveOrThrow(long size);

  long unreserve(long size);

  void inactivate();
}
