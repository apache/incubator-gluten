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
package org.apache.flink.streaming.runtime.translators;

import org.apache.gluten.streaming.api.operators.GlutenStreamSource;
import org.apache.gluten.table.runtime.operators.GlutenSourceFunction;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;

import io.github.zhztheplayer.velox4j.connector.NexmarkConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.NexmarkTableHandle;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TransformationTranslator} for the {@link SourceTransformation}.
 *
 * @param <OUT> The type of the elements that this source produces.
 */
@Internal
public class SourceTransformationTranslator<OUT, SplitT extends SourceSplit, EnumChkT>
    extends SimpleTransformationTranslator<OUT, SourceTransformation<OUT, SplitT, EnumChkT>> {

  @Override
  protected Collection<Integer> translateForBatchInternal(
      final SourceTransformation<OUT, SplitT, EnumChkT> transformation, final Context context) {

    return translateInternal(
        transformation, context, false /* don't emit progressive watermarks */);
  }

  @Override
  protected Collection<Integer> translateForStreamingInternal(
      final SourceTransformation<OUT, SplitT, EnumChkT> transformation, final Context context) {

    return translateInternal(transformation, context, true /* emit progressive watermarks */);
  }

  private Collection<Integer> translateInternal(
      final SourceTransformation<OUT, SplitT, EnumChkT> transformation,
      final Context context,
      boolean emitProgressiveWatermarks) {
    checkNotNull(transformation);
    checkNotNull(context);

    final StreamGraph streamGraph = context.getStreamGraph();
    final String slotSharingGroup = context.getSlotSharingGroup();
    final int transformationId = transformation.getId();
    final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();

    // --- Begin Gluten-specific code changes ---
    if (transformation.getSource().getClass().getSimpleName().equals("NexmarkSource")) {
      RowType outputType =
          (RowType)
              LogicalTypeConverter.toVLType(
                  ((InternalTypeInfo) transformation.getOutputType()).toLogicalType());
      String id = PlanNodeIdGenerator.newId();
      StreamOperatorFactory<OUT> operatorFactory =
          SimpleOperatorFactory.of(
              new GlutenStreamSource(
                  new GlutenSourceFunction(
                      new TableScanNode(
                          id, outputType, new NexmarkTableHandle("connector-nexmark"), List.of()),
                      outputType,
                      id,
                      // TODO: should use config to get parameters
                      new NexmarkConnectorSplit("connector-nexmark", 100000000))));
      streamGraph.addLegacySource(
          transformationId,
          slotSharingGroup,
          transformation.getCoLocationGroupKey(),
          operatorFactory,
          null,
          transformation.getOutputType(),
          "Source: " + transformation.getName());
    } else {
      SourceOperatorFactory<OUT> operatorFactory =
          new SourceOperatorFactory<>(
              transformation.getSource(),
              transformation.getWatermarkStrategy(),
              emitProgressiveWatermarks);

      operatorFactory.setChainingStrategy(transformation.getChainingStrategy());
      operatorFactory.setCoordinatorListeningID(transformation.getCoordinatorListeningID());

      streamGraph.addSource(
          transformationId,
          slotSharingGroup,
          transformation.getCoLocationGroupKey(),
          operatorFactory,
          null,
          transformation.getOutputType(),
          "Source: " + transformation.getName());
    }
    // --- End Gluten-specific code changes ---

    final int parallelism =
        transformation.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT
            ? transformation.getParallelism()
            : executionConfig.getParallelism();

    streamGraph.setParallelism(
        transformationId, parallelism, transformation.isParallelismConfigured());
    streamGraph.setMaxParallelism(transformationId, transformation.getMaxParallelism());

    streamGraph.setSupportsConcurrentExecutionAttempts(
        transformationId, transformation.isSupportsConcurrentExecutionAttempts());

    return Collections.singleton(transformationId);
  }
}
