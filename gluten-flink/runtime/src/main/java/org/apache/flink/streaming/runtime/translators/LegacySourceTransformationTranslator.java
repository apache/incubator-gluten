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

import io.github.zhztheplayer.velox4j.connector.FuzzerConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.FuzzerTableHandle;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.InputFormatOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Gluten {@link TransformationTranslator} for the {@link LegacySourceTransformation}.
 *
 * @param <OUT> The type of the elements that the {@link LegacySourceTransformation} we are
 *     translating is producing.
 */
@Internal
public class LegacySourceTransformationTranslator<OUT>
    extends SimpleTransformationTranslator<OUT, LegacySourceTransformation<OUT>> {

  @Override
  protected Collection<Integer> translateForBatchInternal(
      final LegacySourceTransformation<OUT> transformation, final Context context) {
    return translateInternal(transformation, context);
  }

  @Override
  protected Collection<Integer> translateForStreamingInternal(
      final LegacySourceTransformation<OUT> transformation, final Context context) {
    return translateInternal(transformation, context);
  }

  private Collection<Integer> translateInternal(
      final LegacySourceTransformation<OUT> transformation, final Context context) {
    checkNotNull(transformation);
    checkNotNull(context);

    final StreamGraph streamGraph = context.getStreamGraph();
    final String slotSharingGroup = context.getSlotSharingGroup();
    final int transformationId = transformation.getId();
    final ExecutionConfig executionConfig = streamGraph.getExecutionConfig();

    Function userFunction = transformation.getOperator().getUserFunction();
    StreamOperatorFactory<OUT> operatorFactory;
    String namePrefix = "";
    // --- Begin Gluten-specific code changes ---
    if (userFunction instanceof DataGeneratorSource) {
      RowType outputType =
          (RowType)
              LogicalTypeConverter.toVLType(
                  ((InternalTypeInfo) transformation.getOutputType()).toLogicalType());
      String id = PlanNodeIdGenerator.newId();
      operatorFactory =
          SimpleOperatorFactory.of(
              new GlutenStreamSource(
                  new GlutenSourceFunction(
                      new TableScanNode(
                          id,
                          outputType,
                          new FuzzerTableHandle("connector-fuzzer", 12367),
                          List.of()),
                      outputType,
                      id,
                      new FuzzerConnectorSplit("connector-fuzzer", 1000))));
      namePrefix = "Gluten ";
    } else {
      operatorFactory = transformation.getOperatorFactory();
    }
    // --- End Gluten-specific code changes ---
    streamGraph.addLegacySource(
        transformationId,
        slotSharingGroup,
        transformation.getCoLocationGroupKey(),
        operatorFactory,
        null,
        transformation.getOutputType(),
        namePrefix + "Source: " + transformation.getName());

    if (transformation.getOperatorFactory() instanceof InputFormatOperatorFactory) {
      streamGraph.setInputFormat(
          transformationId,
          ((InputFormatOperatorFactory<OUT>) transformation.getOperatorFactory()).getInputFormat());
    }

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
