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
package org.apache.gluten.memory.memtarget;

import org.apache.gluten.memory.SimpleMemoryUsageRecorder;
import org.apache.gluten.proto.MemoryUsageStats;

public class NoopMemoryTarget implements MemoryTarget, KnownNameAndStats {
  private final SimpleMemoryUsageRecorder recorder = new SimpleMemoryUsageRecorder();
  private final String name = MemoryTargetUtil.toUniqueName("Noop");

  @Override
  public long borrow(long size) {
    recorder.inc(size);
    return size;
  }

  @Override
  public long repay(long size) {
    recorder.inc(-size);
    return size;
  }

  @Override
  public long usedBytes() {
    return recorder.current();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public MemoryUsageStats stats() {
    return recorder.toStats();
  }
}
