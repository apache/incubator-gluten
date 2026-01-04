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

import org.apache.flink.configuration.Configuration;
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
    visitAndFoldChainSlice(sourceChainSlice, chainSliceGraph, targetChainSliceGraph, 0);
    visitAndUpdateStreamEdges(sourceChainSlice, chainSliceGraph, targetChainSliceGraph);
    serializeAllOperatorsConfigs(targetChainSliceGraph);
    targetChainSliceGraph.dumpLog();

    StreamConfig sourceConfig = sourceChainSlice.getOperatorConfigs().get(0);
    StreamConfig targetSourceConfig =
        targetChainSliceGraph.getSlice(sourceChainSlice.id()).getOperatorConfigs().get(0);

    Map<Integer, StreamConfig> chainedConfig = new HashMap<Integer, StreamConfig>();
    if (sourceChainSlice.isOffloadable()) {
      sourceConfig.setStreamOperatorFactory(
          targetSourceConfig.getStreamOperatorFactory(userClassloader));
      List<StreamEdge> chainedOutputs = targetSourceConfig.getChainedOutputs(userClassloader);
      sourceConfig.setChainedOutputs(targetSourceConfig.getChainedOutputs(userClassloader));
      sourceConfig.setTypeSerializerOut(targetSourceConfig.getTypeSerializerOut(userClassloader));
    } else {
      List<StreamConfig> operatorConfigs = sourceChainSlice.getOperatorConfigs();
      for (int i = 0; i < operatorConfigs.size(); i++) {
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
  private void visitAndFoldChainSlice(
      OperatorChainSlice chainSlice,
      OperatorChainSliceGraph originalChainSliceGraph,
      OperatorChainSliceGraph targetChainSliceGraph,
      Integer chainedIndex) {
    List<Integer> outputs = chainSlice.getOutputs();
    List<Integer> outputIndex = new ArrayList<>();
    OperatorChainSlice resultChainSlice = null;
    if (chainSlice.isOffloadable()) {
      resultChainSlice =
          foldOffloadableOperatorChainSlice(originalChainSliceGraph, chainSlice, chainedIndex);
      chainedIndex = chainedIndex + 1;
    } else {
      resultChainSlice = foldUnoffloadableOperatorChainSlice(chainSlice, chainedIndex);
      chainedIndex = chainedIndex + chainSlice.getOperatorConfigs().size();
    }

    resultChainSlice.getInputs().addAll(chainSlice.getInputs());
    resultChainSlice.getOutputs().addAll(chainSlice.getOutputs());
    targetChainSliceGraph.addSlice(chainSlice.id(), resultChainSlice);

    for (Integer outputChainIndex : outputs) {
      OperatorChainSlice outputChainSlice = originalChainSliceGraph.getSlice(outputChainIndex);
      OperatorChainSlice outputResultChainSlice = targetChainSliceGraph.getSlice(outputChainIndex);
      if (outputResultChainSlice == null) {
        visitAndFoldChainSlice(
            outputChainSlice, originalChainSliceGraph, targetChainSliceGraph, chainedIndex);
      }
    }
  }

  private OperatorChainSlice foldUnoffloadableOperatorChainSlice(
      OperatorChainSlice originalChainSlice, Integer chainedIndex) {
    OperatorChainSlice resultChainSlice = new OperatorChainSlice(originalChainSlice.id());
    List<StreamConfig> operatorConfigs = originalChainSlice.getOperatorConfigs();
    for (StreamConfig opConfig : operatorConfigs) {
      StreamConfig newOpConfig = new StreamConfig(new Configuration(opConfig.getConfiguration()));
      newOpConfig.setChainIndex(chainedIndex);
      resultChainSlice.getOperatorConfigs().add(newOpConfig);
    }
    resultChainSlice.setOffloadable(false);
    return resultChainSlice;
  }

  // Fold offloadable operator chain slice, and update the input/output channel serializers
  private OperatorChainSlice foldOffloadableOperatorChainSlice(
      OperatorChainSliceGraph chainSliceGraph,
      OperatorChainSlice originalChainSlice,
      Integer chainedIndex) {
    OperatorChainSlice resultChainSlice = new OperatorChainSlice(originalChainSlice.id());
    List<StreamConfig> operatorConfigs = originalChainSlice.getOperatorConfigs();

    if (operatorConfigs.size() != 1) {
      throw new UnsupportedOperationException(
          "Only one operator is supported for offloaded operator chain slice.");
    }

    StreamConfig rootOpConfig = operatorConfigs.get(0);
    GlutenOperator rootOp = getGlutenOperator(rootOpConfig).get();
    StatefulPlanNode rootPlanNode = rootOp.getPlanNode();
    StreamConfig newRootOpConfig =
        new StreamConfig(new Configuration(rootOpConfig.getConfiguration()));
    if (rootOp instanceof GlutenStreamSource) {
      boolean couldOutputRowVector = couldOutputRowVector(originalChainSlice, chainSliceGraph);
      Class<?> outClass = couldOutputRowVector ? StatefulRecord.class : RowData.class;
      GlutenStreamSource newSourceOp =
          new GlutenStreamSource(
              new GlutenSourceFunction<>(
                  rootPlanNode,
                  rootOp.getOutputTypes(),
                  rootOp.getId(),
                  ((GlutenStreamSource) rootOp).getConnectorSplit(),
                  outClass));
      newRootOpConfig.setStreamOperator(newSourceOp);
      if (couldOutputRowVector) {
        RowType rowType = rootOp.getOutputTypes().entrySet().iterator().next().getValue();
        newRootOpConfig.setTypeSerializerOut(new GlutenStatefulRecordSerializer(rowType));
      }
    } else if (rootOp instanceof GlutenOneInputOperator) {
      boolean couldOutputRowVector = couldOutputRowVector(originalChainSlice, chainSliceGraph);
      boolean couldInputRowVector = couldInputRowVector(originalChainSlice, chainSliceGraph);
      Class<?> inClass = couldInputRowVector ? StatefulRecord.class : RowData.class;
      Class<?> outClass = couldOutputRowVector ? StatefulRecord.class : RowData.class;
      GlutenOneInputOperator<?, ?> newOneInputOp =
          new GlutenOneInputOperator<>(
              rootPlanNode,
              rootOp.getId(),
              rootOp.getInputType(),
              rootOp.getOutputTypes(),
              inClass,
              outClass);
      newRootOpConfig.setStreamOperator(newOneInputOp);
      if (couldOutputRowVector) {
        RowType rowType = rootOp.getOutputTypes().entrySet().iterator().next().getValue();
        newRootOpConfig.setTypeSerializerOut(new GlutenStatefulRecordSerializer(rowType));
      }
      if (couldInputRowVector) {
        newRootOpConfig.setupNetworkInputs(
            new GlutenStatefulRecordSerializer(rootOp.getInputType()));
      }
    } else if (rootOp instanceof GlutenTwoInputOperator) {
      GlutenTwoInputOperator<?, ?> twoInputOp = (GlutenTwoInputOperator<?, ?>) rootOp;
      boolean couldOutputRowVector = couldOutputRowVector(originalChainSlice, chainSliceGraph);
      boolean couldInputRowVector = couldInputRowVector(originalChainSlice, chainSliceGraph);
      Class<?> inClass = couldInputRowVector ? StatefulRecord.class : RowData.class;
      Class<?> outClass = couldOutputRowVector ? StatefulRecord.class : RowData.class;
      GlutenTwoInputOperator<?, ?> newTwoInputOp =
          new GlutenTwoInputOperator<>(
              rootPlanNode,
              twoInputOp.getLeftId(),
              twoInputOp.getRightId(),
              twoInputOp.getLeftInputType(),
              twoInputOp.getRightInputType(),
              twoInputOp.getOutputTypes(),
              inClass,
              outClass);
      newRootOpConfig.setStreamOperator(newTwoInputOp);
      newRootOpConfig.setStatePartitioner(0, new GlutenKeySelector());
      newRootOpConfig.setStatePartitioner(1, new GlutenKeySelector());
      // Update the output channel serializer
      if (couldOutputRowVector) {
        RowType rowType = twoInputOp.getOutputTypes().entrySet().iterator().next().getValue();
        newRootOpConfig.setTypeSerializerOut(new GlutenStatefulRecordSerializer(rowType));
      }
      // Update the input channel serializers
      if (couldInputRowVector) {
        newRootOpConfig.setupNetworkInputs(
            new GlutenStatefulRecordSerializer(twoInputOp.getLeftInputType()),
            new GlutenStatefulRecordSerializer(twoInputOp.getRightInputType()));
      }
    } else {
      throw new UnsupportedOperationException(
          "Only GlutenStreamSource is supported for offloaded operator chain slice.");
    }

    newRootOpConfig.setChainIndex(chainedIndex);
    resultChainSlice.getOperatorConfigs().add(newRootOpConfig);
    resultChainSlice.setOffloadable(true);
    return resultChainSlice;
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

  // Incase the vetexs has been changed, update the stream edges.
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
      OperatorChainSlice chainSlice, OperatorChainSliceGraph chainSliceGraph) {
    boolean could = true;
    for (Integer outputID : chainSlice.getOutputs()) {
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
    return could;
  }

  boolean couldInputRowVector(
      OperatorChainSlice chainSlice, OperatorChainSliceGraph chainSliceGraph) {
    boolean could = true;
    for (Integer inputID : chainSlice.getInputs()) {
      OperatorChainSlice inputChainSlice = chainSliceGraph.getSlice(inputID);
      if (!inputChainSlice.isOffloadable()) {
        could = false;
        break;
      }
      if (!couldOutputRowVector(inputChainSlice, chainSliceGraph)) {
        could = false;
        break;
      }
    }
    return could;
  }
}
