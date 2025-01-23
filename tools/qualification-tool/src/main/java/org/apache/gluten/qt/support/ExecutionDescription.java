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
package org.apache.gluten.qt.support;

import org.apache.gluten.qt.graph.MetricInternal;
import org.apache.gluten.qt.graph.SparkPlanGraphClusterInternal;
import org.apache.gluten.qt.graph.SparkPlanGraphInternal;
import org.apache.gluten.qt.graph.SparkPlanGraphNodeInternal;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toCollection;

/**
 * A container describing the structure and relationships of a Spark plan graph, including:
 *
 * <ul>
 *   <li>A mapping of node IDs to their children nodes (edges)
 *   <li>A mapping of node IDs to the corresponding {@link SparkPlanGraphNodeInternal} instances
 *   <li>A mapping of accumulator IDs to their metric values (from {@link MetricInternal})
 *   <li>Cluster membership details, indicating which nodes belong to a {@link
 *       SparkPlanGraphClusterInternal}
 *   <li>The earliest non-cluster node ID, designated as the “start node”
 *   <li>A mapping of node IDs to their {@link GlutenSupport} statuses
 * </ul>
 *
 * <p>The primary constructor processes a {@link SparkPlanGraphInternal} along with its metrics to
 * build these mappings. Each node, cluster, and metric in the graph is recorded internally.
 * Subclasses of {@link GraphVisitor} may then traverse this structure, updating or reading the node
 * support statuses.
 */
public class ExecutionDescription {
  private final Map<Long, List<Long>> nodeIdToEdges;
  private final Map<Long, SparkPlanGraphNodeInternal> nodeIdToNodeMap;
  private final Map<Long, String> accumulatorIdToMetricMap;
  private final Map<Long, SparkPlanGraphClusterInternal> nodeIdToClusterMap;
  private final long startNodeId;
  private final Map<Long, GlutenSupport> nodeIdToGluttenSupportMap;

  private ExecutionDescription(
      Map<Long, List<Long>> nodeIdToEdges,
      Map<Long, SparkPlanGraphNodeInternal> nodeIdToNodeMap,
      Map<Long, String> accumulatorIdToMetricMap,
      Map<Long, SparkPlanGraphClusterInternal> nodeIdToClusterMap,
      long startNodeId,
      Map<Long, GlutenSupport> nodeIdToGluttenSupportMap) {
    this.nodeIdToEdges = nodeIdToEdges;
    this.nodeIdToNodeMap = nodeIdToNodeMap;
    this.accumulatorIdToMetricMap = accumulatorIdToMetricMap;
    this.nodeIdToClusterMap = nodeIdToClusterMap;
    this.startNodeId = startNodeId;
    this.nodeIdToGluttenSupportMap = nodeIdToGluttenSupportMap;
  }

  ExecutionDescription withNewSupportMap(Map<Long, GlutenSupport> newSupportMap) {
    return new ExecutionDescription(
        nodeIdToEdges,
        nodeIdToNodeMap,
        accumulatorIdToMetricMap,
        nodeIdToClusterMap,
        startNodeId,
        newSupportMap);
  }

  public ExecutionDescription(SparkPlanGraphInternal graph, List<MetricInternal> metrics) {
    this.nodeIdToEdges =
        graph.getEdges().stream()
            .collect(
                Collectors.groupingBy(
                    e -> e.getToId(),
                    Collectors.mapping(e -> e.getFromId(), toCollection(ArrayList::new))));
    this.nodeIdToNodeMap =
        graph.getAllNodes().stream()
            .collect(ImmutableMap.toImmutableMap(SparkPlanGraphNodeInternal::getId, node -> node));

    this.accumulatorIdToMetricMap =
        metrics.stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    MetricInternal::getKey, MetricInternal::getMetricValue));

    this.nodeIdToClusterMap =
        graph.getNodes().stream()
            .filter(SparkPlanGraphClusterInternal.class::isInstance)
            .map(SparkPlanGraphClusterInternal.class::cast)
            .flatMap(
                cluster ->
                    cluster.getNodes().stream().map(node -> Map.entry(node.getId(), cluster)))
            .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    this.startNodeId =
        Collections.min(
            nodeIdToNodeMap.entrySet().stream()
                .filter(kv -> !(kv.getValue() instanceof SparkPlanGraphClusterInternal))
                .map(Map.Entry::getKey)
                .collect(ImmutableList.toImmutableList()));
    this.nodeIdToGluttenSupportMap = new HashMap<>();
  }

  public long getStartNodeId() {
    return startNodeId;
  }

  public Map<Long, GlutenSupport> getNodeIdToGluttenSupportMap() {
    return nodeIdToGluttenSupportMap;
  }

  public List<Long> getChildren(long nodeId) {
    return nodeIdToEdges.getOrDefault(nodeId, Collections.emptyList());
  }

  public SparkPlanGraphNodeInternal getNode(long nodeId) {
    return nodeIdToNodeMap.get(nodeId);
  }

  public String getMetrics(long accumulatorId) {
    return accumulatorIdToMetricMap.get(accumulatorId);
  }

  public boolean isNodeInCluster(long nodeId) {
    return nodeIdToClusterMap.containsKey(nodeId);
  }

  public SparkPlanGraphClusterInternal getCluster(long nodeId) {
    return nodeIdToClusterMap.get(nodeId);
  }
}
