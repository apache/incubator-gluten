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

import io.glutenproject.memory.MemoryUsageStatsBuilder;
import io.glutenproject.memory.SimpleMemoryUsageRecorder;
import io.glutenproject.proto.MemoryUsageStats;

import com.google.common.base.Preconditions;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.util.Utils;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is a Spark memory consumer and at the same time a factory to create sub-targets that share
 * one fixed memory capacity.
 *
 * <p>Once full (size > capacity), spillers will be called by the consumer with order. If failed to
 * free out enough memory, throw OOM to caller.
 *
 * <p>Spark's memory manager could either trigger spilling on the children spillers since this was
 * registered as a Spark memory consumer.
 *
 * <p>Typically used by utility class {@link
 * io.glutenproject.memory.memtarget.spark.IsolatedMemoryConsumers}.
 */
public class TreeMemoryConsumer extends MemoryConsumer implements TreeMemoryConsumerNode {
  private final SimpleMemoryUsageRecorder recorder = new SimpleMemoryUsageRecorder();
  private final Map<String, TreeMemoryConsumerNode> children = new HashMap<>();

  TreeMemoryConsumer(TaskMemoryManager taskMemoryManager) {
    super(taskMemoryManager, taskMemoryManager.pageSizeBytes(), MemoryMode.OFF_HEAP);
  }

  @Override
  public long borrow(long size) {
    if (size == 0) {
      // or Spark complains about the zero size by throwing an error
      return 0;
    }
    long acquired = acquireMemory(size);
    recorder.inc(acquired);
    return acquired;
  }

  @Override
  public long repay(long size) {
    if (size == 0) {
      return 0;
    }
    freeMemory(size);
    Preconditions.checkArgument(getUsed() >= 0);
    recorder.inc(-size);
    return size;
  }

  @Override
  public String name() {
    return "Gluten.Tree@" + Integer.toHexString(System.identityHashCode(this));
  }

  @Override
  public long usedBytes() {
    return getUsed();
  }

  @Override
  public MemoryUsageStats stats() {
    Set<Map.Entry<String, TreeMemoryConsumerNode>> entries = children.entrySet();
    Map<String, MemoryUsageStats> childrenStats =
        entries.stream()
            .collect(Collectors.toMap(e -> e.getValue().name(), e -> e.getValue().stats()));

    Preconditions.checkState(childrenStats.size() == children.size());
    MemoryUsageStats stats = recorder.toStats(childrenStats);
    Preconditions.checkState(
        stats.getCurrent() == getUsed(),
        "Used bytes mismatch between gluten memory consumer and Spark task memory manager");
    return stats;
  }

  @Override
  public TaskMemoryManager getTaskMemoryManager() {
    return taskMemoryManager;
  }

  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    // subject to the regular Spark spill calls
    return spillTree(this, size);
  }

  static long spillTree(TreeMemoryConsumerNode node, final long bytes) {
    // sort children by used bytes, descending
    Queue<TreeMemoryConsumerNode> q =
        new PriorityQueue<>(
            (o1, o2) -> {
              long diff = o1.usedBytes() - o2.usedBytes();
              return -(diff > 0 ? 1 : diff < 0 ? -1 : 0); // descending
            });
    q.addAll(node.children().values());

    long remainingBytes = bytes;
    while (q.peek() != null && remainingBytes > 0) {
      TreeMemoryConsumerNode head = q.remove();
      long spilled = spillTree(head, remainingBytes);
      remainingBytes -= spilled;
    }

    if (remainingBytes > 0) {
      // if still doesn't fit, spill self
      long spilled = node.getNodeSpiller().spill(remainingBytes);
      remainingBytes -= spilled;
    }

    return bytes - remainingBytes;
  }

  @Override
  public TreeMemoryConsumerNode newChild(
      String name,
      long capacity,
      Spiller spiller,
      Map<String, MemoryUsageStatsBuilder> virtualChildren) {
    if (children.containsKey(name)) {
      throw new IllegalArgumentException("Child already registered: " + name);
    }
    Node child = new Node(this, name, capacity, spiller, virtualChildren);
    children.put(name, child);
    return child;
  }

  @Override
  public Map<String, TreeMemoryConsumerNode> children() {
    return Collections.unmodifiableMap(children);
  }

  @Override
  public TreeMemoryConsumerNode parent() {
    // we are root
    throw new IllegalStateException("Unreachable code");
  }

  @Override
  public Spiller getNodeSpiller() {
    // root doesn't spill
    return Spiller.NO_OP;
  }

  // non-root nodes are not Spark memory consumer
  private class Node implements TreeMemoryConsumerNode {
    private final Map<String, TreeMemoryConsumerNode> children = new HashMap<>();
    private final TreeMemoryConsumerNode parent;
    private final String name;
    private final long capacity;
    private final Spiller spiller;
    private final Map<String, MemoryUsageStatsBuilder> virtualChildren;
    private final SimpleMemoryUsageRecorder selfRecorder = new SimpleMemoryUsageRecorder();

    private Node(
        TreeMemoryConsumerNode parent,
        String name,
        long capacity,
        Spiller spiller,
        Map<String, MemoryUsageStatsBuilder> virtualChildren) {
      this.parent = parent;
      this.name = name;
      this.capacity = capacity;
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

    public Spiller getNodeSpiller() {
      return spiller;
    }

    private boolean ensureFreeCapacity(long bytesNeeded) {
      long prevFreeBytes = -1L;
      while (true) { // FIXME should we add retry limit?
        long freeBytes = freeBytes();
        Preconditions.checkState(freeBytes >= 0);
        if (freeBytes >= bytesNeeded) {
          // free bytes fit requirement
          return true;
        }
        // we are about to OOM
        if (freeBytes == prevFreeBytes) {
          // OOM
          return false;
        }
        prevFreeBytes = freeBytes;
        // spill
        long bytesToSpill = bytesNeeded - freeBytes;
        spillTree(this, bytesToSpill);
      }
    }

    @Override
    public long repay(long size) {
      selfRecorder.inc(-size);
      long freed = parent.repay(size);
      Preconditions.checkState(freed == size, "Freed size is not equal to requested size");
      return size;
    }

    @Override
    public long usedBytes() {
      return selfRecorder.current();
    }

    @Override
    public TaskMemoryManager getTaskMemoryManager() {
      return TreeMemoryConsumer.this.getTaskMemoryManager();
    }

    @Override
    public String name() {
      if (capacity == CAPACITY_UNLIMITED) {
        return name;
      }
      return String.format("%s, %s", name, Utils.bytesToString(capacity));
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
    public TreeMemoryConsumerNode newChild(
        String name,
        long capacity,
        Spiller spiller,
        Map<String, MemoryUsageStatsBuilder> virtualChildren) {
      if (children.containsKey(name)) {
        throw new IllegalArgumentException("Child already registered: " + name);
      }
      Node child = new Node(this, name, capacity, spiller, virtualChildren);
      children.put(name, child);
      return child;
    }

    @Override
    public Map<String, TreeMemoryConsumerNode> children() {
      return Collections.unmodifiableMap(children);
    }

    @Override
    public TreeMemoryConsumerNode parent() {
      return parent;
    }
  }
}
