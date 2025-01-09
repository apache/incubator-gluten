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

import com.google.common.collect.ImmutableMap;

import java.util.stream.Collectors;

import static org.apache.gluten.qt.support.NotSupportedCategory.CLUSTER_NOT_SUPPORTED;

/**
 * {@link GraphVisitor} that evaluates support for entire clusters within the {@link
 * ExecutionDescription}. If a node is part of a cluster:
 *
 * <ul>
 *   <li>If all nodes within the cluster are supported, this class marks each node in that cluster
 *       as supported.
 *   <li>If any node in the cluster is not supported, this class updates all nodes in that cluster
 *       to be {@link NotSupported} and records the relevant reasons.
 * </ul>
 *
 * If a node does not belong to a cluster, its support status is taken directly from the {@code
 * NodeIdToGluttenSupportMap}.
 */
public class ClusterSupportVisitor extends GraphVisitor {
  public ClusterSupportVisitor(ExecutionDescription executionDescription) {
    super(executionDescription);
  }

  @Override
  protected void visitor(long nodeId) {
    if (!resultSupportMap.containsKey(nodeId) && executionDescription.isNodeInCluster(nodeId)) {
      ImmutableMap<Long, GlutenSupport> clusterSupport =
          executionDescription.getCluster(nodeId).getNodes().stream()
              .collect(
                  ImmutableMap.toImmutableMap(
                      n -> n.getId(),
                      n -> executionDescription.getNodeIdToGluttenSupportMap().get(n.getId())));
      ImmutableMap<Long, NotSupported> notSupportedMap =
          clusterSupport.entrySet().stream()
              .filter(s -> s.getValue() instanceof NotSupported)
              .collect(
                  ImmutableMap.toImmutableMap(e -> e.getKey(), e -> (NotSupported) e.getValue()));

      if (notSupportedMap.isEmpty()) {
        resultSupportMap.putAll(clusterSupport);
      } else {
        String newDescription =
            notSupportedMap.keySet().stream()
                .map(id -> executionDescription.getNode(id).getName())
                .distinct()
                .collect(Collectors.joining(":"));
        ImmutableMap<Long, GlutenSupport> newEntries =
            clusterSupport.entrySet().stream()
                .collect(
                    ImmutableMap.toImmutableMap(
                        e -> e.getKey(),
                        e -> {
                          if (e.getValue() instanceof NotSupported) {
                            NotSupported ns = (NotSupported) e.getValue();
                            ns.addReason(CLUSTER_NOT_SUPPORTED, newDescription);
                            return ns;
                          } else {
                            return new NotSupported(CLUSTER_NOT_SUPPORTED, newDescription);
                          }
                        }));
        resultSupportMap.putAll(newEntries);
      }
    } else if (!resultSupportMap.containsKey(nodeId)
        && !executionDescription.isNodeInCluster(nodeId)) {
      resultSupportMap.put(nodeId, executionDescription.getNodeIdToGluttenSupportMap().get(nodeId));
    }
  }
}
