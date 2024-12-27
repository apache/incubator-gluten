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
package org.apache.gluten.hash;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsistentHashTest {

  private ConsistentHash<ConsistentHash.Node> consistentHash;
  private static final int REPLICAS = 3;

  @Before
  public void setUp() throws Exception {
    consistentHash = new ConsistentHash<>(REPLICAS);
  }

  @Test
  public void testAddNode() {
    Set<ConsistentHash.Node> nodes =
        IntStream.range(0, 10)
            .mapToObj(i -> new ConsistentHashTest.HostNode(String.format("executor-%d", i)))
            .collect(Collectors.toSet());
    nodes.forEach(n -> consistentHash.addNode(n));
    Assert.assertEquals(10, consistentHash.getNodes().size());

    HostNode existsNode = new HostNode("executor-1");
    HostNode nonExistsNode = new HostNode("executor-100");
    Assert.assertTrue(consistentHash.contains(existsNode));
    Assert.assertFalse(consistentHash.contains(nonExistsNode));

    Set<ConsistentHash.Partition<ConsistentHash.Node>> existsPartitions =
        consistentHash.getPartition(existsNode);
    Assert.assertEquals(REPLICAS, existsPartitions.size());
    Set<ConsistentHash.Partition<ConsistentHash.Node>> nonExistsPartitions =
        consistentHash.getPartition(nonExistsNode);
    Assert.assertNull(nonExistsPartitions);
  }

  @Test
  public void testRemoveNode() {
    Set<ConsistentHash.Node> nodes =
        IntStream.range(0, 10)
            .mapToObj(i -> new ConsistentHashTest.HostNode(String.format("executor-%d", i)))
            .collect(Collectors.toSet());
    nodes.forEach(n -> consistentHash.addNode(n));
    HostNode existsNode = new HostNode("executor-11");
    consistentHash.addNode(existsNode);
    Assert.assertEquals(11, consistentHash.getNodes().size());
    Set<ConsistentHash.Partition<ConsistentHash.Node>> partitions =
        consistentHash.getPartition(existsNode);
    Assert.assertEquals(REPLICAS, partitions.size());
    ConsistentHash.Partition<ConsistentHash.Node> partition = partitions.iterator().next();
    Assert.assertTrue(consistentHash.ringContain(partition.getSlot()));
    consistentHash.removeNode(existsNode);
    Assert.assertEquals(10, consistentHash.getNodes().size());
    Set<ConsistentHash.Partition<ConsistentHash.Node>> removedPartitions =
        consistentHash.getPartition(existsNode);
    Assert.assertNull(removedPartitions);
    Assert.assertFalse(consistentHash.ringContain(partition.getSlot()));
  }

  @Test
  public void testContain() {
    Set<ConsistentHash.Node> nodes =
        IntStream.range(0, 10)
            .mapToObj(i -> new ConsistentHashTest.HostNode(String.format("executor-%d", i)))
            .collect(Collectors.toSet());
    nodes.forEach(n -> consistentHash.addNode(n));
    HostNode existsNode = new HostNode("executor-11");
    consistentHash.addNode(existsNode);
    Assert.assertTrue(consistentHash.contains(existsNode));
  }

  @Test
  public void testAllocateNodes() {
    Set<ConsistentHash.Node> nodes =
        IntStream.range(0, 10)
            .mapToObj(i -> new ConsistentHashTest.HostNode(String.format("executor-%d", i)))
            .collect(Collectors.toSet());
    nodes.forEach(n -> consistentHash.addNode(n));

    Set<ConsistentHash.Node> allocateNodes =
        consistentHash.allocateNodes("part-00000-38af6778-964a-4a86-b1f9-8bf783cc65aa-c000", 3);
    Assert.assertEquals(3, allocateNodes.size());
    for (ConsistentHash.Node node : allocateNodes) {
      Assert.assertTrue(consistentHash.contains(node));
    }

    Set<ConsistentHash.Node> allocateAllNodes =
        consistentHash.allocateNodes("part-00000-38af6778-964a-4a86-b1f9-8bf783cc65aa-c000", 11);
    Assert.assertEquals(10, allocateAllNodes.size());
    for (ConsistentHash.Node node : allocateAllNodes) {
      Assert.assertTrue(nodes.contains(node));
    }
  }

  private static class HostNode implements ConsistentHash.Node {
    private final String host;

    HostNode(String host) {
      this.host = host;
    }

    @Override
    public String key() {
      return host;
    }

    @Override
    public String toString() {
      return host;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(host);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof HostNode)) return false;
      HostNode that = (HostNode) o;
      return Objects.equals(host, that.host);
    }
  }
}
