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
import org.apache.gluten.memory.SimpleMemoryUsageRecorder;
import org.apache.gluten.proto.MemoryUsageStats;

import com.google.common.base.Preconditions;
import org.apache.spark.util.Utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.Collectors;

public class TreeMemoryTargets {

  private TreeMemoryTargets() {
    // enclose factory ctor
  }

  public static TreeMemoryTarget newChild(
      TreeMemoryTarget parent,
      String name,
      long capacity,
      Spiller spiller,
      Map<String, MemoryUsageStatsBuilder> virtualChildren) {
    return new Node(parent, name, capacity, spiller, virtualChildren);
  }

  public static long spillTree(TreeMemoryTarget node, final long bytes) {
    long remainingBytes = bytes;
    for (Spiller.Phase phase : Spiller.Phase.values()) {
      // First shrink, then if no good, spill.
      if (remainingBytes <= 0) {
        break;
      }
      remainingBytes -= spillTree(node, phase, remainingBytes);
    }
    return bytes - remainingBytes;
  }

  private static long spillTree(TreeMemoryTarget node, Spiller.Phase phase, final long bytes) {
    // sort children by used bytes, descending
    Queue<TreeMemoryTarget> q =
        new PriorityQueue<>(
            (o1, o2) -> {
              long diff = o1.usedBytes() - o2.usedBytes();
              return -(diff > 0 ? 1 : diff < 0 ? -1 : 0); // descending
            });
    q.addAll(node.children().values());

    long remainingBytes = bytes;
    while (q.peek() != null && remainingBytes > 0) {
      TreeMemoryTarget head = q.remove();
      long spilled = spillTree(head, remainingBytes);
      remainingBytes -= spilled;
    }

    if (remainingBytes > 0) {
      // if still doesn't fit, spill self
      final Spiller spiller = node.getNodeSpiller();
      long spilled = spiller.spill(node, phase, remainingBytes);
      remainingBytes -= spilled;
    }

    return bytes - remainingBytes;
  }

  // non-root nodes are not Spark memory consumer
  public static class Node implements TreeMemoryTarget, KnownNameAndStats {
    private final Map<String, Node> children = new HashMap<>();
    private final TreeMemoryTarget parent;
    private final String name;
    private final long capacity;
    private final Spiller spiller;
    private final Map<String, MemoryUsageStatsBuilder> virtualChildren;
    private final SimpleMemoryUsageRecorder selfRecorder = new SimpleMemoryUsageRecorder();

    private Node(
        TreeMemoryTarget parent,
        String name,
        long capacity,
        Spiller spiller,
        Map<String, MemoryUsageStatsBuilder> virtualChildren) {
      this.parent = parent;
      this.capacity = capacity;
      final String uniqueName = MemoryTargetUtil.toUniqueName(name);
      if (capacity == CAPACITY_UNLIMITED) {
        this.name = uniqueName;
      } else {
        this.name = String.format("%s, %s", uniqueName, Utils.bytesToString(capacity));
      }
      this.spiller = spiller;
      this.virtualChildren = virtualChildren;
    }

    @Override
    public long borrow(long size) {
      ensureFreeCapacity(size);
      return borrow0(Math.min(freeBytes(), size));
    }

    private long freeBytes() {
      return capacity - usedBytes();
    }

    private long borrow0(long size) {
      long granted = parent.borrow(size);
      selfRecorder.inc(granted);
      return granted;
    }

    @Override
    public Spiller getNodeSpiller() {
      return spiller;
    }

    private boolean ensureFreeCapacity(long bytesNeeded) {
      while (true) { // FIXME should we add retry limit?
        long freeBytes = freeBytes();
        Preconditions.checkState(freeBytes >= 0);
        if (freeBytes >= bytesNeeded) {
          // free bytes fit requirement
          return true;
        }
        // spill
        long bytesToSpill = bytesNeeded - freeBytes;
        long spilledBytes = TreeMemoryTargets.spillTree(this, bytesToSpill);
        Preconditions.checkState(spilledBytes >= 0);
        if (spilledBytes == 0) {
          // OOM
          return false;
        }
      }
    }

    @Override
    public long repay(long size) {
      long toFree = Math.min(usedBytes(), size);
      long freed = parent.repay(toFree);
      selfRecorder.inc(-freed);
      return freed;
    }

    @Override
    public long usedBytes() {
      return selfRecorder.current();
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
      final Map<String, MemoryUsageStats> childrenStats =
          new HashMap<>(
              children.entrySet().stream()
                  .collect(Collectors.toMap(e -> e.getValue().name(), e -> e.getValue().stats())));

      Preconditions.checkState(childrenStats.size() == children.size());

      // add virtual children
      for (Map.Entry<String, MemoryUsageStatsBuilder> entry : virtualChildren.entrySet()) {
        if (childrenStats.containsKey(entry.getKey())) {
          throw new IllegalArgumentException("Child stats already exists: " + entry.getKey());
        }
        childrenStats.put(entry.getKey(), entry.getValue().toStats());
      }
      return selfRecorder.toStats(childrenStats);
    }

    @Override
    public TreeMemoryTarget newChild(
        String name,
        long capacity,
        Spiller spiller,
        Map<String, MemoryUsageStatsBuilder> virtualChildren) {
      final Node child = new Node(this, name, capacity, spiller, virtualChildren);
      if (children.containsKey(child.name())) {
        throw new IllegalArgumentException("Child already registered: " + child.name());
      }
      children.put(child.name(), child);
      return child;
    }

    @Override
    public Map<String, TreeMemoryTarget> children() {
      return Collections.unmodifiableMap(children);
    }

    @Override
    public TreeMemoryTarget parent() {
      return parent;
    }
  }
}
