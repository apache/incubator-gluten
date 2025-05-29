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
import org.apache.gluten.table.runtime.operators.GlutenSingleInputOperator;
import org.apache.gluten.table.runtime.operators.GlutenSourceFunction;

import io.github.zhztheplayer.velox4j.plan.PlanNode;

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  // A JobVertex may contain several operators chained like this: Source-->Op1-->Op2-->Sink.
  // If the operators connected all support translated to gluten, we merge them into
  // a single GlutenOperator to avoid data transferred between flink and native.
  // Now we only support that one operator followed by at most one other operator.
  private void buildGlutenChains(StreamConfig vertexConfig) {
    Map<Integer, StreamConfig> serializedTasks =
        vertexConfig.getTransitiveChainedTaskConfigs(userClassloader);
    Map<Integer, StreamConfig> chainedTasks = new HashMap<>(serializedTasks.size());
    serializedTasks.forEach(
        (id, config) -> chainedTasks.put(id, new StreamConfig(config.getConfiguration())));
    StreamConfig taskConfig = vertexConfig;
    while (true) {
      List<StreamEdge> outEdges = taskConfig.getChainedOutputs(userClassloader);
      if (outEdges == null || outEdges.size() != 1) {
        // only support operators have one output.
        LOG.debug("{} has no or more than one chained task.", taskConfig.getOperatorName());
        break;
      }

      StreamEdge outEdge = outEdges.get(0);
      StreamConfig outTask = chainedTasks.get(outEdge.getTargetId());
      if (outTask == null) {
        LOG.warn("Not find task {} in Chained tasks", outEdge.getTargetId());
        break;
      }
      if (isGlutenOperator(taskConfig) && isGlutenOperator(outTask)) {
        GlutenOperator outOperator = getGlutenOperator(outTask);
        PlanNode outNode = outOperator.getPlanNode();
        GlutenOperator sourceOperator = getGlutenOperator(taskConfig);
        if (outNode != null) {
          outNode.setSources(List.of(sourceOperator.getPlanNode()));
          LOG.debug("Set {} source to {}", outNode, sourceOperator.getPlanNode());
        } else {
          outNode = sourceOperator.getPlanNode();
          LOG.debug("Set out node to {}", sourceOperator.getPlanNode());
        }
        if (sourceOperator instanceof GlutenStreamSource) {
          GlutenStreamSource streamSource = (GlutenStreamSource) sourceOperator;
          taskConfig.setStreamOperator(
              new GlutenStreamSource(
                  new GlutenSourceFunction(
                      outNode,
                      outOperator.getOutputType(),
                      sourceOperator.getId(),
                      streamSource.getConnectorSplit())));
        } else {
          taskConfig.setStreamOperator(
              new GlutenSingleInputOperator(
                  outNode,
                  outOperator.getId(),
                  sourceOperator.getInputType(),
                  outOperator.getOutputType()));
        }
        taskConfig.setChainedOutputs(outTask.getChainedOutputs(userClassloader));
        taskConfig.setOperatorNonChainedOutputs(
            outTask.getOperatorNonChainedOutputs(userClassloader));
        taskConfig.serializeAllConfigs();
      } else {
        LOG.debug(
            "{} and {} can not be merged", taskConfig.getOperatorName(), outTask.getOperatorName());
        taskConfig = outTask;
      }
    }
    // TODO: may need fallback if failed.
    vertexConfig.setAndSerializeTransitiveChainedTaskConfigs(chainedTasks);
  }

  private boolean isGlutenOperator(StreamConfig taskConfig) {
    StreamOperatorFactory operatorFactory = taskConfig.getStreamOperatorFactory(userClassloader);
    if (operatorFactory instanceof SimpleOperatorFactory) {
      return taskConfig.getStreamOperator(userClassloader) instanceof GlutenOperator;
    } else if (operatorFactory instanceof GlutenOneInputOperatorFactory) {
      return true;
    }
    return false;
  }

  private GlutenOperator getGlutenOperator(StreamConfig taskConfig) {
    StreamOperatorFactory operatorFactory = taskConfig.getStreamOperatorFactory(userClassloader);
    if (operatorFactory instanceof SimpleOperatorFactory) {
      return taskConfig.getStreamOperator(userClassloader);
    } else if (operatorFactory instanceof GlutenOneInputOperatorFactory) {
      return ((GlutenOneInputOperatorFactory) operatorFactory).getOperator();
    }
    return null;
  }
  // --- End Gluten-specific code changes ---
}
