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
import org.apache.gluten.streaming.api.operators.GlutenStreamSource;
import org.apache.gluten.table.runtime.keyselector.GlutenKeySelector;
import org.apache.gluten.table.runtime.operators.GlutenOneInputOperator;
import org.apache.gluten.table.runtime.operators.GlutenSourceFunction;
import org.apache.gluten.table.runtime.operators.GlutenTwoInputOperator;
import org.apache.gluten.table.runtime.typeutils.GlutenStatefulRecordSerializer;

import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/*
 * If a operator is offloadable
 * - update its input/output serializers as needed.
 * - update its key selectors as needed.
 * - coalesce it with its siblings as needed. Also need to update the stream edges.
 */
public class OffloadedJobGraphGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(OffloadedJobGraphGenerator.class);
  private final JobGraph jobGraph;
  private final ClassLoader userClassloader;
  private boolean hasGenerated = false;

  public OffloadedJobGraphGenerator(JobGraph jobGraph, ClassLoader userClassloader) {
    this.jobGraph = jobGraph;
    this.userClassloader = userClassloader;
  }

  public JobGraph generate() {
    if (hasGenerated) {
      throw new IllegalStateException("JobGraph has been generated.");
    }
    hasGenerated = true;
    for (JobVertex jobVertex : jobGraph.getVertices()) {
      offloadJobVertex(jobVertex);
    }
    return jobGraph;
  }

  private void offloadJobVertex(JobVertex jobVertex) {
    OperatorChainSliceGraphGenerator graphGenerator =
        new OperatorChainSliceGraphGenerator(jobVertex, userClassloader);
    OperatorChainSliceGraph chainSliceGraph = graphGenerator.getGraph();
    chainSliceGraph.dumpLog();

    OperatorChainSlice sourceChainSlice = chainSliceGraph.getSourceSlice();
    OperatorChainSliceGraph targetChainSliceGraph = new OperatorChainSliceGraph();
    visitAndOffloadChainOperators(
        sourceChainSlice, chainSliceGraph, targetChainSliceGraph, 0, jobVertex);
    visitAndUpdateStreamEdges(sourceChainSlice, chainSliceGraph, targetChainSliceGraph);
    serializeAllOperatorsConfigs(targetChainSliceGraph);

    StreamConfig sourceConfig = sourceChainSlice.getOperatorConfigs().get(0);
    StreamConfig targetSourceConfig =
        targetChainSliceGraph.getSlice(sourceChainSlice.id()).getOperatorConfigs().get(0);

    Map<Integer, StreamConfig> chainedConfig = new HashMap<Integer, StreamConfig>();
    if (sourceChainSlice.isOffloadable()) {
      // Update the first operator config
      sourceConfig.setStreamOperatorFactory(
          targetSourceConfig.getStreamOperatorFactory(userClassloader));
      sourceConfig.setChainedOutputs(targetSourceConfig.getChainedOutputs(userClassloader));

      // Update the serializers and partitioners
      sourceConfig.setTypeSerializerOut(targetSourceConfig.getTypeSerializerOut(userClassloader));
      sourceConfig.setInputs(targetSourceConfig.getInputs(userClassloader));
      KeySelector<?, ?> keySelector0 = targetSourceConfig.getStatePartitioner(0, userClassloader);
      if (keySelector0 != null) {
        sourceConfig.setStatePartitioner(0, keySelector0);
      }
      KeySelector<?, ?> keySelector1 = targetSourceConfig.getStatePartitioner(1, userClassloader);
      if (keySelector1 != null) {
        sourceConfig.setStatePartitioner(1, keySelector1);
      }

      // The chained operators should be empty.
    } else {
      List<StreamConfig> operatorConfigs = sourceChainSlice.getOperatorConfigs();
      for (int i = 1; i < operatorConfigs.size(); i++) {
        StreamConfig opConfig = operatorConfigs.get(i);
        chainedConfig.put(opConfig.getVertexID(), opConfig);
      }
    }
    for (OperatorChainSlice chainSlice : targetChainSliceGraph.getSlices().values()) {
      if (chainSlice.id().equals(sourceChainSlice.id())) {
        continue;
      }
      List<StreamConfig> operatorConfigs = chainSlice.getOperatorConfigs();
      for (StreamConfig opConfig : operatorConfigs) {
        chainedConfig.put(opConfig.getVertexID(), opConfig);
      }
    }
    sourceConfig.setAndSerializeTransitiveChainedTaskConfigs(chainedConfig);
    sourceConfig.serializeAllConfigs();
  }

  // Fold offloadable operator chain slice
  private void visitAndOffloadChainOperators(
      OperatorChainSlice chainSlice,
      OperatorChainSliceGraph originalChainSliceGraph,
      OperatorChainSliceGraph targetChainSliceGraph,
      Integer chainedIndex,
      JobVertex jobVertex) {
    List<Integer> outputs = chainSlice.getOutputs();
    OperatorChainSlice finalChainSlice = null;
    if (chainSlice.isOffloadable()) {
      finalChainSlice =
          OffloadOperatorChainSlice(originalChainSliceGraph, chainSlice, chainedIndex, jobVertex);
      chainedIndex = chainedIndex + 1;
    } else {
      finalChainSlice = applyUnoffloadableOperatorChainSlice(chainSlice, chainedIndex);
      chainedIndex = chainedIndex + chainSlice.getOperatorConfigs().size();
    }

    finalChainSlice.getInputs().addAll(chainSlice.getInputs());
    finalChainSlice.getOutputs().addAll(chainSlice.getOutputs());
    targetChainSliceGraph.addSlice(chainSlice.id(), finalChainSlice);

    for (Integer outputChainIndex : outputs) {
      OperatorChainSlice outputChainSlice = originalChainSliceGraph.getSlice(outputChainIndex);
      OperatorChainSlice outputResultChainSlice = targetChainSliceGraph.getSlice(outputChainIndex);
      if (outputResultChainSlice == null) {
        visitAndOffloadChainOperators(
            outputChainSlice,
            originalChainSliceGraph,
            targetChainSliceGraph,
            chainedIndex,
            jobVertex);
      }
    }
  }

  // Keep the original operator chain slice as is.
  private OperatorChainSlice applyUnoffloadableOperatorChainSlice(
      OperatorChainSlice originalChainSlice, Integer chainedIndex) {
    OperatorChainSlice finalChainSlice = new OperatorChainSlice(originalChainSlice.id());
    List<StreamConfig> operatorConfigs = originalChainSlice.getOperatorConfigs();
    for (StreamConfig opConfig : operatorConfigs) {
      StreamConfig newOpConfig = new StreamConfig(new Configuration(opConfig.getConfiguration()));
      newOpConfig.setChainIndex(chainedIndex);
      finalChainSlice.getOperatorConfigs().add(newOpConfig);
    }
    finalChainSlice.setOffloadable(false);
    return finalChainSlice;
  }

  // Fold offloadable operator chain slice, and update the input/output channel serializers
  private OperatorChainSlice OffloadOperatorChainSlice(
      OperatorChainSliceGraph chainSliceGraph,
      OperatorChainSlice originalChainSlice,
      Integer chainedIndex,
      JobVertex jobVertex) {
    OperatorChainSlice finalChainSlice = new OperatorChainSlice(originalChainSlice.id());
    List<StreamConfig> operatorConfigs = originalChainSlice.getOperatorConfigs();

    // May coalesce multiple operators into one in the future.
    if (operatorConfigs.size() != 1) {
      throw new UnsupportedOperationException(
          "Only one operator is supported for offloaded operator chain slice.");
    }

    StreamConfig originalOpConfig = operatorConfigs.get(0);
    GlutenOperator originalOp = getGlutenOperator(originalOpConfig).get();
    StatefulPlanNode planNode = originalOp.getPlanNode();
    // Create a new operator config for the offloaded operator.
    StreamConfig finalOpConfig =
        new StreamConfig(new Configuration(originalOpConfig.getConfiguration()));
    if (originalOp instanceof GlutenStreamSource) {
      boolean couldOutputRowVector =
          couldOutputRowVector(originalChainSlice, chainSliceGraph, jobVertex);
      Class<?> outClass = couldOutputRowVector ? StatefulRecord.class : RowData.class;
      GlutenStreamSource newSourceOp =
          new GlutenStreamSource(
              new GlutenSourceFunction<>(
                  planNode,
                  originalOp.getOutputTypes(),
                  originalOp.getId(),
                  ((GlutenStreamSource) originalOp).getConnectorSplit(),
                  outClass));
      finalOpConfig.setStreamOperator(newSourceOp);
      if (couldOutputRowVector) {
        RowType rowType = originalOp.getOutputTypes().entrySet().iterator().next().getValue();
        finalOpConfig.setTypeSerializerOut(
            new GlutenStatefulRecordSerializer(rowType, originalOp.getId()));
      }
    } else if (originalOp instanceof GlutenOneInputOperator) {
      boolean couldOutputRowVector =
          couldOutputRowVector(originalChainSlice, chainSliceGraph, jobVertex);
      boolean couldInputRowVector =
          couldInputRowVector(originalChainSlice, chainSliceGraph, jobVertex);
      Class<?> inClass = couldInputRowVector ? StatefulRecord.class : RowData.class;
      Class<?> outClass = couldOutputRowVector ? StatefulRecord.class : RowData.class;
      GlutenOneInputOperator<?, ?> newOneInputOp =
          new GlutenOneInputOperator<>(
              planNode,
              originalOp.getId(),
              originalOp.getInputType(),
              originalOp.getOutputTypes(),
              inClass,
              outClass,
              originalOp.getDescription());
      finalOpConfig.setStreamOperator(newOneInputOp);
      if (couldOutputRowVector) {
        RowType rowType = originalOp.getOutputTypes().entrySet().iterator().next().getValue();
        finalOpConfig.setTypeSerializerOut(
            new GlutenStatefulRecordSerializer(rowType, originalOp.getId()));
      }
      if (couldInputRowVector) {
        finalOpConfig.setupNetworkInputs(
            new GlutenStatefulRecordSerializer(originalOp.getInputType(), originalOp.getId()));

        // This node is the first node in the chain. If it has a state partitioner, we need to
        // change it to GlutenKeySelector.
        KeySelector<?, ?> keySelector = originalOpConfig.getStatePartitioner(0, userClassloader);
        if (keySelector != null) {
          LOG.info(
              "State partitioner ({}) found in the first node {}, change it to GlutenKeySelector.",
              keySelector.getClass().getName(),
              originalOp.getDescription());
          finalOpConfig.setStatePartitioner(0, new GlutenKeySelector());
        }
      }
    } else if (originalOp instanceof GlutenTwoInputOperator) {
      GlutenTwoInputOperator<?, ?> twoInputOp = (GlutenTwoInputOperator<?, ?>) originalOp;
      boolean couldOutputRowVector =
          couldOutputRowVector(originalChainSlice, chainSliceGraph, jobVertex);
      boolean couldInputRowVector =
          couldInputRowVector(originalChainSlice, chainSliceGraph, jobVertex);
      KeySelector<?, ?> keySelector0 = originalOpConfig.getStatePartitioner(0, userClassloader);
      if (keySelector0 != null) {
        LOG.info(
            "State partitioner ({}) found in the first node {}, change it to GlutenKeySelector.",
            keySelector0.getClass().getName(),
            originalOp.getDescription());
        finalOpConfig.setStatePartitioner(0, new GlutenKeySelector());
      }
      KeySelector<?, ?> keySelector1 = originalOpConfig.getStatePartitioner(1, userClassloader);
      if (keySelector1 != null) {
        LOG.info(
            "State partitioner ({}) found in the second node {}, change it to GlutenKeySelector.",
            keySelector1.getClass().getName(),
            originalOp.getDescription());
        finalOpConfig.setStatePartitioner(1, new GlutenKeySelector());
      }
      Class<?> inClass = couldInputRowVector ? StatefulRecord.class : RowData.class;
      Class<?> outClass = couldOutputRowVector ? StatefulRecord.class : RowData.class;
      GlutenTwoInputOperator<?, ?> newTwoInputOp =
          new GlutenTwoInputOperator<>(
              planNode,
              twoInputOp.getLeftId(),
              twoInputOp.getRightId(),
              twoInputOp.getLeftInputType(),
              twoInputOp.getRightInputType(),
              twoInputOp.getOutputTypes(),
              inClass,
              outClass);
      finalOpConfig.setStreamOperator(newTwoInputOp);
      finalOpConfig.setStatePartitioner(0, new GlutenKeySelector());
      finalOpConfig.setStatePartitioner(1, new GlutenKeySelector());
      // Update the output channel serializer
      if (couldOutputRowVector) {
        RowType rowType = twoInputOp.getOutputTypes().entrySet().iterator().next().getValue();
        finalOpConfig.setTypeSerializerOut(
            new GlutenStatefulRecordSerializer(rowType, twoInputOp.getId()));
      }
      // Update the input channel serializers
      if (couldInputRowVector) {
        finalOpConfig.setupNetworkInputs(
            new GlutenStatefulRecordSerializer(twoInputOp.getLeftInputType(), twoInputOp.getId()),
            new GlutenStatefulRecordSerializer(twoInputOp.getRightInputType(), twoInputOp.getId()));
      }
    } else {
      throw new UnsupportedOperationException(
          "Only GlutenStreamSource is supported for offloaded operator chain slice.");
    }

    finalOpConfig.setChainIndex(chainedIndex);
    finalChainSlice.getOperatorConfigs().add(finalOpConfig);
    finalChainSlice.setOffloadable(true);
    return finalChainSlice;
  }

  private StreamNode mockStreamNode(StreamConfig streamConfig) {
    return new StreamNode(
        streamConfig.getVertexID(),
        null,
        null,
        (StreamOperatorFactory<?>) streamConfig.getStreamOperatorFactory(userClassloader),
        streamConfig.getOperatorName(),
        null);
  }

  // In case the vertices has been changed, update the stream edges.
  private void visitAndUpdateStreamEdges(
      OperatorChainSlice originalChainSlice,
      OperatorChainSliceGraph originalChainSliceGraph,
      OperatorChainSliceGraph targetChainSliceGraph) {
    OperatorChainSlice targetChainSlice = targetChainSliceGraph.getSlice(originalChainSlice.id());
    if (targetChainSlice.isOffloadable()) {
      List<Integer> outputIDs = originalChainSlice.getOutputs();
      List<StreamConfig> operatorConfigs = targetChainSlice.getOperatorConfigs();
      StreamConfig targetOpConfig = operatorConfigs.get(0);
      if (outputIDs.size() == 0) {
        targetOpConfig.setChainedOutputs(new ArrayList<>());
        return;
      }
      List<StreamEdge> newOutputEdges = new ArrayList<>();
      List<StreamEdge> originalOutputEdges =
          originalChainSlice
              .getOperatorConfigs()
              .get(originalChainSlice.getOperatorConfigs().size() - 1)
              .getChainedOutputs(userClassloader);
      for (int i = 0; i < outputIDs.size(); i++) {
        Integer outputID = outputIDs.get(i);
        OperatorChainSlice outputOriginalChainSlice = originalChainSliceGraph.getSlice(outputID);
        OperatorChainSlice outputTargetChainSlice = targetChainSliceGraph.getSlice(outputID);
        StreamConfig outputOpConfig =
            outputTargetChainSlice
                .getOperatorConfigs()
                .get(0); // The first operator config is the representative.
        StreamEdge originalEdge = originalOutputEdges.get(i);
        StreamEdge newEdge =
            new StreamEdge(
                mockStreamNode(targetOpConfig),
                mockStreamNode(outputOpConfig),
                originalEdge.getTypeNumber(),
                originalEdge.getBufferTimeout(),
                originalEdge.getPartitioner(),
                originalEdge.getOutputTag(),
                originalEdge.getExchangeMode(),
                0,
                originalEdge.getIntermediateDatasetIdToProduce());
        newOutputEdges.add(newEdge);
      }
      targetOpConfig.setChainedOutputs(newOutputEdges);
    }

    for (Integer outputChain : originalChainSlice.getOutputs()) {
      visitAndUpdateStreamEdges(
          originalChainSliceGraph.getSlice(outputChain),
          originalChainSliceGraph,
          targetChainSliceGraph);
    }
  }

  void serializeAllOperatorsConfigs(OperatorChainSliceGraph chainSliceGraph) {
    for (OperatorChainSlice chainSlice : chainSliceGraph.getSlices().values()) {
      List<StreamConfig> operatorConfigs = chainSlice.getOperatorConfigs();
      for (StreamConfig opConfig : operatorConfigs) {
        opConfig.serializeAllConfigs();
      }
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

  boolean isAllOffloadable(OperatorChainSliceGraph chainSliceGraph, List<Integer> chainIDs) {
    for (Integer chainID : chainIDs) {
      OperatorChainSlice chainSlice = chainSliceGraph.getSlice(chainID);
      if (!chainSlice.isOffloadable()) {
        return false;
      }
    }
    return true;
  }

  boolean couldOutputRowVector(
      OperatorChainSlice chainSlice, OperatorChainSliceGraph chainSliceGraph, JobVertex jobVertex) {
    boolean could = true;
    List<Integer> outputs = chainSlice.getOutputs();

    // If chainSlice has downstream in the operator chain, check these downstream
    if (!outputs.isEmpty()) {
      for (Integer outputID : outputs) {
        OperatorChainSlice outputChainSlice = chainSliceGraph.getSlice(outputID);
        if (!outputChainSlice.isOffloadable()) {
          could = false;
          break;
        }
        List<Integer> inputs = outputChainSlice.getInputs();
        if (!isAllOffloadable(chainSliceGraph, inputs)) {
          could = false;
          break;
        }
      }
    } else {
      // If chainSlice has no downstream in the operator chain, get downstream JobVertex from
      // JobGraph level
      // Use the provided jobVertex directly
      List<JobVertex> downstreamVertices = getDownstreamJobVertices(jobVertex);
      if (downstreamVertices.isEmpty()) {
        // If there is no downstream JobVertex, can output RowVector (no downstream needs
        // conversion)
        could = true;
      } else {
        // Check if downstream vertex's operator is gluten operator
        for (JobVertex downstreamVertex : downstreamVertices) {
          StreamConfig downstreamStreamConfig =
              new StreamConfig(downstreamVertex.getConfiguration());
          Optional<GlutenOperator> glutenOperator = getGlutenOperator(downstreamStreamConfig);
          if (!glutenOperator.isPresent()) {
            could = false;
            break;
          }
        }
      }
    }
    return could;
  }

  boolean couldInputRowVector(
      OperatorChainSlice chainSlice, OperatorChainSliceGraph chainSliceGraph, JobVertex jobVertex) {
    boolean could = true;
    List<Integer> inputs = chainSlice.getInputs();

    // If chainSlice has upstream in the operator chain, check these upstream
    if (!inputs.isEmpty()) {
      for (Integer inputID : inputs) {
        OperatorChainSlice inputChainSlice = chainSliceGraph.getSlice(inputID);
        if (!inputChainSlice.isOffloadable()) {
          could = false;
          break;
        }
        if (!couldOutputRowVector(inputChainSlice, chainSliceGraph, jobVertex)) {
          could = false;
          break;
        }
      }
    } else {
      // If chainSlice has no upstream in the operator chain, get upstream JobVertex from JobGraph
      // level
      // Use the provided jobVertex directly
      List<JobVertex> upstreamVertices = getUpstreamJobVertices(jobVertex);
      if (upstreamVertices.isEmpty()) {
        // If there is no upstream JobVertex, can input RowVector (no upstream needs conversion)
        could = true;
      } else {
        // Check if the last operator of upstream vertex is gluten operator
        for (JobVertex upstreamVertex : upstreamVertices) {
          StreamConfig upstreamStreamConfig = new StreamConfig(upstreamVertex.getConfiguration());
          Map<Integer, StreamConfig> chainedConfigs =
              upstreamStreamConfig.getTransitiveChainedTaskConfigs(userClassloader);
          chainedConfigs.put(upstreamStreamConfig.getVertexID(), upstreamStreamConfig);

          // Find the last operator (the one with no chained outputs)
          StreamConfig lastOpConfig = null;
          for (StreamConfig config : chainedConfigs.values()) {
            List<StreamEdge> chainedOutputs = config.getChainedOutputs(userClassloader);
            if (chainedOutputs == null || chainedOutputs.isEmpty()) {
              lastOpConfig = config;
              break;
            }
          }

          // If no last operator found, use the root config
          if (lastOpConfig == null) {
            lastOpConfig = upstreamStreamConfig;
          }

          Optional<GlutenOperator> glutenOperator = getGlutenOperator(lastOpConfig);
          if (!glutenOperator.isPresent()) {
            could = false;
            break;
          }
        }
      }
    }
    return could;
  }

  /** Get all downstream JobVertices of a JobVertex. */
  private List<JobVertex> getDownstreamJobVertices(JobVertex vertex) {
    List<JobVertex> downstreamVertices = new ArrayList<>();
    for (IntermediateDataSet dataSet : vertex.getProducedDataSets()) {
      for (JobEdge edge : dataSet.getConsumers()) {
        JobVertex downstreamVertex = edge.getTarget();
        downstreamVertices.add(downstreamVertex);
      }
    }
    return downstreamVertices;
  }

  /** Get all upstream JobVertices of a JobVertex. */
  private List<JobVertex> getUpstreamJobVertices(JobVertex vertex) {
    List<JobVertex> upstreamVertices = new ArrayList<>();
    for (JobEdge edge : vertex.getInputs()) {
      JobVertex upstreamVertex = edge.getSource().getProducer();
      upstreamVertices.add(upstreamVertex);
    }
    return upstreamVertices;
  }
}
