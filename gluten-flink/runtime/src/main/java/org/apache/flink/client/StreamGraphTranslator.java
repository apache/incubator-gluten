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

import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.gluten.streaming.api.operators.GlutenOneInputOperatorFactory;
import org.apache.gluten.table.runtime.operators.GlutenSourceFunction;

import io.github.zhztheplayer.velox4j.plan.PlanNode;
import org.apache.gluten.streaming.api.operators.GlutenOperator;
import org.apache.gluten.streaming.api.operators.GlutenStreamSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.HashMap;
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

    private JobGraph mergeGlutenOperators(JobGraph jobGraph) {
        for (JobVertex vertex : jobGraph.getVertices()) {
            StreamConfig streamConfig = new StreamConfig(vertex.getConfiguration());
            buildGlutenChains(streamConfig);
        }
        return jobGraph;
    }

    private void buildGlutenChains(
            StreamConfig taskConfig) {
        // TODO: only support head operator now.
        if (isGlutenOperator(taskConfig)) {
            while (true) {
                List<StreamEdge> outEdges = taskConfig.getChainedOutputs(userClassloader);
                if (outEdges.size() != 1) {
                    // only support operators have one output.
                    break;
                }
                StreamEdge outEdge = outEdges.get(0);
                Map<Integer, StreamConfig> chainedTasks =
                        taskConfig.getTransitiveChainedTaskConfigs(userClassloader);
                StreamConfig outTask = chainedTasks.get(outEdge.getTargetId());
                if (outTask != null) {
                    if (isGlutenOperator(outTask)) {
                        GlutenOperator outOperator = getGlutenOperator(outTask);
                        PlanNode outNode = outOperator.getPlanNode();
                        GlutenOperator sourceOperator = getGlutenOperator(taskConfig);
                        if (outNode != null) {
                            outNode.setSources(List.of(sourceOperator.getPlanNode()));
                        } else {
                            outNode = sourceOperator.getPlanNode();
                        }
                        if (sourceOperator instanceof GlutenStreamSource) {
                            taskConfig.setStreamOperator(
                                    new GlutenStreamSource(
                                            new GlutenSourceFunction(
                                                    outNode,
                                                    outOperator.getOutputType(),
                                                    sourceOperator.getId())));
                        } else {
                            // TODO: support GlutenCombinedOperator.
                            //taskConfig.setStreamOperator(
                            //        new GlutenCombinedOperator(outNode));
                        }
                        // TODO: stream edges need to be reset.
                        taskConfig.setTransitiveChainedTaskConfigs(
                                outTask.getTransitiveChainedTaskConfigs(userClassloader));
                        taskConfig.setChainedOutputs(outTask.getChainedOutputs(userClassloader));
                        taskConfig.setOperatorNonChainedOutputs(
                                outTask.getOperatorNonChainedOutputs(userClassloader));
                        taskConfig.serializeAllConfigs();
                    } else {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
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
}
