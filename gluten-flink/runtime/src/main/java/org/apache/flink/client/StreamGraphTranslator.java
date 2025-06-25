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
package org.apache.flink.client;

import org.apache.gluten.streaming.api.operators.GlutenOneInputOperatorFactory;
import org.apache.gluten.streaming.api.operators.GlutenOperator;
import org.apache.gluten.streaming.api.operators.GlutenStreamSource;
import org.apache.gluten.table.runtime.keyselector.GlutenKeySelector;
import org.apache.gluten.table.runtime.operators.GlutenVectorOneInputOperator;
import org.apache.gluten.table.runtime.operators.GlutenVectorSourceFunction;
import org.apache.gluten.table.runtime.operators.GlutenVectorTwoInputOperator;
import org.apache.gluten.table.runtime.typeutils.GlutenRowVectorSerializer;
import org.apache.gluten.util.Utils;

import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * {@link FlinkPipelineTranslator} for DataStream API {@link StreamGraph StreamGraphs}.
 *
 * <p>Note: this is used through reflection in {@link
 * org.apache.flink.client.FlinkPipelineTranslationUtil}.
 */
@SuppressWarnings("unused")
public class StreamGraphTranslator implements FlinkPipelineTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(StreamGraphTranslator.class);

  private final ClassLoader userClassloader;

  public StreamGraphTranslator(ClassLoader userClassloader) {
    this.userClassloader = userClassloader;
  }

  @Override
  public JobGraph translateToJobGraph(
      Pipeline pipeline, Configuration optimizerConfiguration, int defaultParallelism) {
    checkArgument(
        pipeline instanceof StreamGraph, "Given pipeline is not a DataStream StreamGraph.");

    StreamGraph streamGraph = (StreamGraph) pipeline;
    JobGraph jobGraph = streamGraph.getJobGraph(userClassloader, null);
    return mergeGlutenOperators(jobGraph);
  }

  @Override
  public String translateToJSONExecutionPlan(Pipeline pipeline) {
    checkArgument(
        pipeline instanceof StreamGraph, "Given pipeline is not a DataStream StreamGraph.");

    StreamGraph streamGraph = (StreamGraph) pipeline;

    return streamGraph.getStreamingPlanAsJSON();
  }

  @Override
  public boolean canTranslate(Pipeline pipeline) {
    return pipeline instanceof StreamGraph;
  }

  // --- Begin Gluten-specific code changes ---
  private JobGraph mergeGlutenOperators(JobGraph jobGraph) {
    for (JobVertex vertex : jobGraph.getVertices()) {
      StreamConfig streamConfig = new StreamConfig(vertex.getConfiguration());
      buildGlutenChains(streamConfig);
      LOG.debug("Vertex {} is {}.", vertex.getName(), streamConfig);
    }
    return jobGraph;
  }

  // A JobVertex may contain several operators chained like this: Source-->Op1-->Op2-->Sink1.
  //                                                                                -->Sink2.
  // If the operators connected all support translated to gluten, we merge them into
  // a single GlutenOperator to avoid data transferred between flink and native.
  // One operator may be followed by several other operators.
  private void buildGlutenChains(StreamConfig vertexConfig) {
    Map<Integer, StreamConfig> serializedTasks =
        vertexConfig.getTransitiveChainedTaskConfigs(userClassloader);
    Map<Integer, StreamConfig> chainedTasks = new HashMap<>(serializedTasks.size());
    serializedTasks.forEach(
        (id, config) -> chainedTasks.put(id, new StreamConfig(config.getConfiguration())));
    buildGlutenChains(vertexConfig, chainedTasks);
    // TODO: may need fallback if failed.
    vertexConfig.setAndSerializeTransitiveChainedTaskConfigs(chainedTasks);
  }

  private void buildGlutenChains(StreamConfig taskConfig, Map<Integer, StreamConfig> chainedTasks) {
    List<StreamEdge> outEdges = taskConfig.getChainedOutputs(userClassloader);
    Optional<GlutenOperator> sourceOperatorOpt = getGlutenOperator(taskConfig);
    GlutenOperator sourceOperator = sourceOperatorOpt.orElse(null);
    boolean isSourceGluten = sourceOperatorOpt.isPresent();
    if (outEdges == null || outEdges.isEmpty()) {
      LOG.debug("{} has no chained task.", taskConfig.getOperatorName());
      // TODO: judge whether can set?
      if (isSourceGluten) {
        if (taskConfig.getOperatorName().equals("exchange-hash")) {
          taskConfig.setTypeSerializerOut(new GlutenRowVectorSerializer(null));
        }
        Map<IntermediateDataSetID, String> nodeToNonChainedOuts = new HashMap<>(outEdges.size());
        taskConfig
            .getOperatorNonChainedOutputs(userClassloader)
            .forEach(edge -> nodeToNonChainedOuts.put(edge.getDataSetId(), sourceOperator.getId()));
        Utils.setNodeToNonChainedOutputs(taskConfig, nodeToNonChainedOuts);
        taskConfig.serializeAllConfigs();
      }
      return;
    }
    Map<String, Integer> nodeToChainedOuts = new HashMap<>(outEdges.size());
    Map<IntermediateDataSetID, String> nodeToNonChainedOuts = new HashMap<>(outEdges.size());
    Map<String, RowType> nodeToOutTypes = new HashMap<>(outEdges.size());
    List<StreamEdge> chainedOutputs = new ArrayList<>(outEdges.size());
    List<NonChainedOutput> nonChainedOutputs = new ArrayList<>(outEdges.size());
    StatefulPlanNode sourceNode = isSourceGluten ? sourceOperator.getPlanNode() : null;
    boolean allGluten = true;
    LOG.debug("Edge size {}, OP {}", outEdges.size(), sourceOperator);
    for (StreamEdge outEdge : outEdges) {
      StreamConfig outTask = chainedTasks.get(outEdge.getTargetId());
      if (outTask == null) {
        LOG.error("Not find task {} in Chained tasks", outEdge.getTargetId());
        allGluten = false;
        break;
      }
      buildGlutenChains(outTask, chainedTasks);
      Optional<GlutenOperator> outOperator = getGlutenOperator(outTask);
      if (isSourceGluten && outOperator.isPresent()) {
        StatefulPlanNode outNode = outOperator.get().getPlanNode();
        if (sourceNode != null) {
          sourceNode.addTarget(outNode);
          LOG.debug("Add {} target {}", sourceNode, outNode);
        } else {
          sourceNode = outNode;
          LOG.debug("Set target node to {}", outNode);
        }
        Map<String, Integer> node2Out = Utils.getNodeToChainedOutputs(outTask, userClassloader);
        if (node2Out != null) {
          nodeToChainedOuts.putAll(node2Out);
          nodeToNonChainedOuts.putAll(Utils.getNodeToNonChainedOutputs(outTask, userClassloader));
        } else {
          outTask
              .getChainedOutputs(userClassloader)
              .forEach(edge -> nodeToChainedOuts.put(outNode.getId(), edge.getTargetId()));
          outTask
              .getOperatorNonChainedOutputs(userClassloader)
              .forEach(edge -> nodeToNonChainedOuts.put(edge.getDataSetId(), outNode.getId()));
        }
        nodeToOutTypes.putAll(outOperator.get().getOutputTypes());
        chainedOutputs.addAll(outTask.getChainedOutputs(userClassloader));
        nonChainedOutputs.addAll(outTask.getOperatorNonChainedOutputs(userClassloader));
      } else {
        allGluten = false;
        LOG.debug(
            "{} and {} can not be merged", taskConfig.getOperatorName(), outTask.getOperatorName());
        break;
      }
    }
    if (allGluten) {
      if (sourceOperator instanceof GlutenStreamSource) {
        GlutenStreamSource streamSource = (GlutenStreamSource) sourceOperator;
        taskConfig.setStreamOperator(
            new GlutenStreamSource(
                new GlutenVectorSourceFunction(
                    sourceNode,
                    nodeToOutTypes,
                    sourceOperator.getId(),
                    streamSource.getConnectorSplit())));
      } else if (sourceOperator instanceof GlutenVectorTwoInputOperator) {
        GlutenVectorTwoInputOperator twoInputOperator =
            (GlutenVectorTwoInputOperator) sourceOperator;
        taskConfig.setStreamOperator(
            new GlutenVectorTwoInputOperator(
                sourceNode,
                twoInputOperator.getLeftId(),
                twoInputOperator.getRightId(),
                twoInputOperator.getLeftInputType(),
                twoInputOperator.getRightInputType(),
                nodeToOutTypes));
        // TODO: judge whether can set?
        taskConfig.setStatePartitioner(0, new GlutenKeySelector());
        taskConfig.setStatePartitioner(1, new GlutenKeySelector());
        taskConfig.setupNetworkInputs(
            new GlutenRowVectorSerializer(null), new GlutenRowVectorSerializer(null));
      } else {
        taskConfig.setStreamOperator(
            new GlutenVectorOneInputOperator(
                sourceNode, sourceOperator.getId(), sourceOperator.getInputType(), nodeToOutTypes));
        // TODO: judge whether can set?
        taskConfig.setStatePartitioner(0, new GlutenKeySelector());
        taskConfig.setupNetworkInputs(new GlutenRowVectorSerializer(null));
      }
      Utils.setNodeToChainedOutputs(taskConfig, nodeToChainedOuts);
      Utils.setNodeToNonChainedOutputs(taskConfig, nodeToNonChainedOuts);
      taskConfig.setChainedOutputs(chainedOutputs);
      taskConfig.setOperatorNonChainedOutputs(nonChainedOutputs);
      taskConfig.serializeAllConfigs();
    }
  }

  private Optional<GlutenOperator> getGlutenOperator(StreamConfig taskConfig) {
    StreamOperatorFactory operatorFactory = taskConfig.getStreamOperatorFactory(userClassloader);
    if (operatorFactory instanceof SimpleOperatorFactory) {
      StreamOperator streamOperator = taskConfig.getStreamOperator(userClassloader);
      if (streamOperator instanceof GlutenOperator) {
        return Optional.of((GlutenOperator) streamOperator);
      }
    } else if (operatorFactory instanceof GlutenOneInputOperatorFactory) {
      return Optional.of(((GlutenOneInputOperatorFactory) operatorFactory).getOperator());
    }
    return Optional.empty();
  }
  // --- End Gluten-specific code changes ---
}
