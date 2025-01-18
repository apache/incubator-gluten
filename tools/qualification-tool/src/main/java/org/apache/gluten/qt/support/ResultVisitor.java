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

import com.google.errorprone.annotations.CanIgnoreReturnValue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.gluten.qt.support.NotSupportedCategory.NODE_NOT_SUPPORTED;

/**
 * A {@link GraphVisitor} implementation that evaluates each node (and its cluster) in an {@link
 * ExecutionDescription} to determine total versus supported SQL time.
 *
 * <p>It tracks any operators marked as {@code NotSupported} (specifically under the {@link
 * NotSupportedCategory#NODE_NOT_SUPPORTED} category), aggregates their impact cost, and calculates
 * cumulative durations for supported and unsupported portions of the SQL plan.
 */
public class ResultVisitor extends GraphVisitor {

  public ResultVisitor(ExecutionDescription executionDescription) {
    super(executionDescription);
  }

  public void visit() {
    visit(executionDescription.getStartNodeId());
  }

  public ExecutionDescription visitAndTag() {
    throw new UnsupportedOperationException();
  }

  /**
   * Represents the cumulative impact of unsupported operators on CPU time and operator count.
   *
   * <p>This class aggregates the total CPU duration spent on unsupported operators and tracks how
   * many such operators were encountered.
   */
  public static class UnsupportedImpact {
    private Duration cumulativeCpuDuration = Duration.ZERO;
    private long count = 0L;

    public Duration getCumulativeCpuDuration() {
      return cumulativeCpuDuration;
    }

    public long getCount() {
      return count;
    }

    public void addCpuDuration(Duration duration) {
      cumulativeCpuDuration = cumulativeCpuDuration.plus(duration);
    }

    public void increment() {
      count++;
    }

    @CanIgnoreReturnValue
    public UnsupportedImpact add(UnsupportedImpact other) {
      this.cumulativeCpuDuration = this.cumulativeCpuDuration.plus(other.cumulativeCpuDuration);
      this.count += other.count;
      return this;
    }
  }

  private final Set<Long> visitedCluster = new HashSet<>();
  private Duration supportedSqlTime = Duration.ZERO;
  private Duration totalSqlTime = Duration.ZERO;
  private final Map<Long, List<Long>> nodeIdToUnsupportedOperatorsMap = new HashMap<>();
  private final Map<Long, UnsupportedImpact> unsupportedOperatorImpactCostMap = new HashMap<>();
  private final Map<Long, String> unsupportedOperatorReasonMap = new HashMap<>();

  private boolean isOperatorNotSupported(long nodeId) {
    GlutenSupport gs = executionDescription.getNodeIdToGluttenSupportMap().get(nodeId);
    return gs instanceof NotSupported && ((NotSupported) gs).isNotSupported(NODE_NOT_SUPPORTED);
  }

  private NotSupported getNotSupportedOperator(long nodeId) {
    GlutenSupport gs = executionDescription.getNodeIdToGluttenSupportMap().get(nodeId);
    return (NotSupported) gs;
  }

  private DurationMetric getClusterDurationFromNodeId(long nodeId) {
    return executionDescription.getCluster(nodeId).getMetrics().stream()
        .flatMap(
            metric ->
                Optional.ofNullable(executionDescription.getMetrics(metric.getAccumulatorId()))
                    .map(DurationMetric::new).stream())
        .findFirst()
        .orElse(new DurationMetric());
  }

  @Override
  protected void visitor(long nodeId) {
    final DurationMetric durationMetric;

    // Add towards duration only once per cluster
    if (executionDescription.isNodeInCluster(nodeId)) {
      long clusterId = executionDescription.getCluster(nodeId).getId();
      durationMetric = getClusterDurationFromNodeId(nodeId);
      if (!visitedCluster.contains(clusterId)) {
        if (executionDescription.getNodeIdToGluttenSupportMap().get(nodeId) instanceof Supported) {
          supportedSqlTime = supportedSqlTime.plus(durationMetric.getDuration());
        }
        totalSqlTime = totalSqlTime.plus(durationMetric.getDuration());
        visitedCluster.add(clusterId);
      }
    } else {
      durationMetric = new DurationMetric();
    }

    // This list will contain IDs of the current node and its children whose category is
    // NODE_NOT_SUPPORTED
    List<Long> currentNodeUnsupportedOperators = new ArrayList<>();
    if (isOperatorNotSupported(nodeId)) {
      currentNodeUnsupportedOperators.add(nodeId);
      unsupportedOperatorImpactCostMap.put(nodeId, new UnsupportedImpact());
      unsupportedOperatorReasonMap.put(
          nodeId, getNotSupportedOperator(nodeId).getCategoryReason(NODE_NOT_SUPPORTED));
    }

    // Add all children whose category is NODE_NOT_SUPPORTED
    for (long childId : executionDescription.getChildren(nodeId)) {
      List<Long> childUnsupportedCauseList = nodeIdToUnsupportedOperatorsMap.get(childId);
      currentNodeUnsupportedOperators.addAll(childUnsupportedCauseList);
    }

    // Penalize the current node and children nodes if they are NODE_NOT_SUPPORTED.
    // The penalty is added because the children being NODE_NOT_SUPPORTED are preventing current
    // node to be pushed to native.
    currentNodeUnsupportedOperators.forEach(
        id -> {
          UnsupportedImpact currentCost = unsupportedOperatorImpactCostMap.get(id);
          currentCost.addCpuDuration(durationMetric.getDuration());
          currentCost.increment();
        });

    nodeIdToUnsupportedOperatorsMap.put(nodeId, currentNodeUnsupportedOperators);
  }

  public Duration getSupportedSqlTime() {
    return supportedSqlTime;
  }

  public Duration getTotalSqlTime() {
    return totalSqlTime;
  }

  public Map<String, UnsupportedImpact> getUnsupportedOperatorImpactCostMap() {
    return unsupportedOperatorImpactCostMap.entrySet().stream()
        .collect(
            Collectors.groupingBy(
                id -> unsupportedOperatorReasonMap.get(id.getKey()),
                Collectors.collectingAndThen(
                    Collectors.reducing(
                        new UnsupportedImpact(), Map.Entry::getValue, UnsupportedImpact::add),
                    impact -> impact)));
  }
}
