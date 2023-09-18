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
package io.glutenproject.memory.memtarget.spark;

public final class Spillers {
  private Spillers() {
    // enclose factory ctor
  }

  // calls the spillers one by one within the order
  public static Spiller withOrder(Spiller... spillers) {
    return (size) -> {
      long remaining = size;
      for (int i = 0; i < spillers.length && remaining > 0; i++) {
        Spiller spiller = spillers[i];
        remaining -= spiller.spill(remaining);
      }
      return size - remaining;
    };
  }

  public static Spiller withMinSpillSize(Spiller spiller, long minSize) {
    return new WithMinSpillSize(spiller, minSize);
  }

  // Minimum spill target size should be larger than spark.gluten.memory.reservationBlockSize,
  // since any release action within size smaller than the block size may not have chance to
  // report back to the Java-side reservation listener.
  private static class WithMinSpillSize implements Spiller {
    private final Spiller delegated;
    private final long minSize;

    private WithMinSpillSize(Spiller delegated, long minSize) {
      this.delegated = delegated;
      this.minSize = minSize;
    }

    @Override
    public long spill(long size) {
      return delegated.spill(Math.max(size, minSize));
    }
  }
}
