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
package org.apache.gluten.client;

import org.apache.gluten.streaming.api.operators.GlutenOneInputOperatorFactory;
import org.apache.gluten.streaming.api.operators.GlutenOperator;

import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class OperatorChainSliceGraphGenerator {
  private OperatorChainSliceGraph chainSliceGraph = null;
  private JobVertex jobVertex;
  private Map<Integer, StreamConfig> chainedConfigs;
  private final ClassLoader userClassloader;

  public OperatorChainSliceGraphGenerator(JobVertex jobVertex, ClassLoader userClassloader) {
    this.jobVertex = jobVertex;
    this.userClassloader = userClassloader;
  }

  public OperatorChainSliceGraph getGraph() {
    generateInternal();
    return chainSliceGraph;
  }

  private void generateInternal() {
    if (chainSliceGraph != null) {
      return;
    }
    chainSliceGraph = new OperatorChainSliceGraph();

    StreamConfig rootOpConfig = new StreamConfig(jobVertex.getConfiguration());

    chainedConfigs = new HashMap<>();
    rootOpConfig
        .getTransitiveChainedTaskConfigs(userClassloader)
        .forEach(
            (id, config) -> {
              chainedConfigs.put(id, new StreamConfig(config.getConfiguration()));
            });
    chainedConfigs.put(rootOpConfig.getVertexID(), rootOpConfig);

    OperatorChainSlice chainSlice = new OperatorChainSlice(rootOpConfig.getVertexID());
    chainSlice.setOffloadable(isOffloadableOperator(rootOpConfig));
    chainSlice.getOperatorConfigs().add(rootOpConfig);
    chainSliceGraph.addSlice(chainSlice.id(), chainSlice);

    advanceOperatorChainSlice(chainSlice, rootOpConfig);
  }

  private void advanceOperatorChainSlice(
      OperatorChainSlice chainSlice, StreamConfig currentOpConfig) {
    List<StreamEdge> outputEdges = currentOpConfig.getChainedOutputs(userClassloader);
    if (outputEdges == null || outputEdges.isEmpty()) {
      return;
    }
    if (outputEdges.size() == 1) {
      Integer targetId = outputEdges.get(0).getTargetId();
      StreamConfig childOpConfig = chainedConfigs.get(targetId);
      // We don't coalesce operators into the same velox plan at present. Each operator is a
      // separate velox plan.
      startNewOperatorChainSlice(chainSlice, childOpConfig);
    } else {
      for (StreamEdge edge : outputEdges) {
        Integer targetId = edge.getTargetId();
        StreamConfig childOpConfig = chainedConfigs.get(targetId);
        startNewOperatorChainSlice(chainSlice, childOpConfig);
      }
    }
  }

  private void startNewOperatorChainSlice(
      OperatorChainSlice parentChainSlice, StreamConfig childOpConfig) {
    Boolean isFirstVisit = false;
    OperatorChainSlice childChainSlice = chainSliceGraph.getSlice(childOpConfig.getVertexID());
    if (childChainSlice == null) {
      isFirstVisit = true;
      childChainSlice = new OperatorChainSlice(childOpConfig.getVertexID());
    }

    parentChainSlice.getOutputs().add(childChainSlice.id());
    childChainSlice.getInputs().add(parentChainSlice.id());
    // If this path has been visited, do not advance again.
    if (isFirstVisit) {
      childChainSlice.setOffloadable(isOffloadableOperator(childOpConfig));
      childChainSlice.getOperatorConfigs().add(childOpConfig);
      chainSliceGraph.addSlice(childOpConfig.getVertexID(), childChainSlice);
      advanceOperatorChainSlice(childChainSlice, childOpConfig);
    }
  }

  private boolean isOffloadableOperator(StreamConfig opConfig) {
    StreamOperatorFactory operatorFactory = opConfig.getStreamOperatorFactory(userClassloader);
    if (operatorFactory instanceof SimpleOperatorFactory) {
      StreamOperator streamOperator = opConfig.getStreamOperator(userClassloader);
      if (streamOperator instanceof GlutenOperator) {
        return true;
      }
    } else if (operatorFactory instanceof GlutenOneInputOperatorFactory) {
      return true;
    }
    return false;
  }
}
