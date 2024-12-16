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

import org.apache.gluten.memory.MemoryUsageStatsBuilder;
import org.apache.gluten.proto.MemoryUsageStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RetryOnOomMemoryTarget implements TreeMemoryTarget {
  private static final Logger LOGGER = LoggerFactory.getLogger(RetryOnOomMemoryTarget.class);
  private final TreeMemoryTarget target;

  RetryOnOomMemoryTarget(TreeMemoryTarget target) {
    this.target = target;
  }

  @Override
  public long borrow(long size) {
    long granted = target.borrow(size);
    if (granted < size) {
      LOGGER.info("Retrying spill require:{} got:{}", size, granted);
      final long spilled = retryingSpill(Long.MAX_VALUE);
      final long remaining = size - granted;
      if (spilled >= remaining) {
        granted += target.borrow(remaining);
      }
      LOGGER.info("Retrying spill spilled:{} final granted:{}", spilled, granted);
    }
    return granted;
  }

  private long retryingSpill(long size) {
    TreeMemoryTarget rootTarget = target;
    while (true) {
      try {
        rootTarget = rootTarget.parent();
      } catch (IllegalStateException e) {
        // Reached the root node
        break;
      }
    }
    return TreeMemoryTargets.spillTree(rootTarget, size);
  }

  @Override
  public long repay(long size) {
    return target.repay(size);
  }

  @Override
  public long usedBytes() {
    return target.usedBytes();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  @Override
  public String name() {
    return target.name();
  }

  @Override
  public MemoryUsageStats stats() {
    return target.stats();
  }

  @Override
  public TreeMemoryTarget newChild(
      String name,
      long capacity,
      Spiller spiller,
      Map<String, MemoryUsageStatsBuilder> virtualChildren) {
    return target.newChild(name, capacity, spiller, virtualChildren);
  }

  @Override
  public Map<String, TreeMemoryTarget> children() {
    return target.children();
  }

  @Override
  public TreeMemoryTarget parent() {
    return target.parent();
  }

  @Override
  public Spiller getNodeSpiller() {
    return target.getNodeSpiller();
  }

  public TreeMemoryTarget target() {
    return target;
  }
}
