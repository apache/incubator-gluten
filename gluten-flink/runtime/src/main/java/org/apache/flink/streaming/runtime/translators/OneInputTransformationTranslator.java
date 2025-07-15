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

import org.apache.gluten.streaming.api.operators.GlutenOneInputOperatorFactory;
import org.apache.gluten.table.runtime.operators.GlutenSingleInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;
import org.apache.gluten.util.ReflectUtils;

import io.github.zhztheplayer.velox4j.connector.CommitStrategy;
import io.github.zhztheplayer.velox4j.connector.FileSystemInsertTableHandle;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.TableWriteNode;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink.BulkFormatBuilder;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * A {@link TransformationTranslator} for the {@link OneInputTransformation}.
 *
 * @param <IN> The type of the elements in the input {@code Transformation} of the transformation to
 *     translate.
 * @param <OUT> The type of the elements that result from the provided {@code
 *     OneInputTransformation}.
 */
@Internal
public final class OneInputTransformationTranslator<IN, OUT>
    extends AbstractOneInputTransformationTranslator<IN, OUT, OneInputTransformation<IN, OUT>> {

  @Override
  public Collection<Integer> translateForBatchInternal(
      final OneInputTransformation<IN, OUT> transformation, final Context context) {
    KeySelector<IN, ?> keySelector = transformation.getStateKeySelector();
    Collection<Integer> ids =
        translateInternal(
            transformation,
            transformation.getOperatorFactory(),
            transformation.getInputType(),
            keySelector,
            transformation.getStateKeyType(),
            context);
    boolean isKeyed = keySelector != null;
    if (isKeyed) {
      BatchExecutionUtils.applyBatchExecutionSettings(
          transformation.getId(), context, StreamConfig.InputRequirement.SORTED);
    }

    return ids;
  }

  @SuppressWarnings("deprecation")
  private List<Integer> fileWriterPartitionIndexes(OneInputStreamOperator<IN, OUT> operator) {
    List<Integer> partitionIndexes = new ArrayList<>();
    try {
      Class<?> streamingFileWriterClazz =
          Class.forName("org.apache.flink.connector.file.table.stream.AbstractStreamingWriter");
      Object bucketsBuilder =
          ReflectUtils.getObjectField(streamingFileWriterClazz, operator, "bucketsBuilder");
      if (bucketsBuilder instanceof BulkFormatBuilder) {
        Object assigner =
            ReflectUtils.getObjectField(BulkFormatBuilder.class, bucketsBuilder, "bucketAssigner");
        Object partitionComputer =
            ReflectUtils.getObjectField(assigner.getClass(), assigner, "computer");
        int[] partitionIndexArray =
            (int[])
                ReflectUtils.getObjectField(
                    partitionComputer.getClass(), partitionComputer, "partitionIndexes");
        partitionIndexes = Arrays.stream(partitionIndexArray).boxed().collect(Collectors.toList());
      }
    } catch (Exception e) {
      throw new FlinkRuntimeException(e);
    }
    return partitionIndexes;
  }

  private List<String> fileWriterPartitionKeys(OneInputStreamOperator<IN, OUT> operator) {
    List<String> partitionKeys = new ArrayList<>();
    try {
      partitionKeys =
          (List<String>)
              ReflectUtils.getObjectField(operator.getClass(), operator, "partitionKeys");
    } catch (Exception e) {
      throw new FlinkRuntimeException(e);
    }
    return partitionKeys;
  }

  private Map<String, String> fileWriterTableParameters(OneInputStreamOperator<IN, OUT> operator) {
    try {
      Configuration configuration =
          (Configuration) ReflectUtils.getObjectField(operator.getClass(), operator, "conf");
      Map<String, String> tableParams = configuration.toMap();
      tableParams.put("fs.file_name_prefix", UUID.randomUUID().toString());
      tableParams.put("fs.writer_task_id", String.valueOf(0));
      return tableParams;
    } catch (Exception e) {
      throw new FlinkRuntimeException(e);
    }
  }

  @Override
  public Collection<Integer> translateForStreamingInternal(
      final OneInputTransformation<IN, OUT> transformation, final Context context) {
    StreamOperatorFactory<OUT> operatorFactory = transformation.getOperatorFactory();
    if (operatorFactory instanceof SimpleOperatorFactory) {
      String operatorClazzName = transformation.getOperator().getClass().getSimpleName();
      if (operatorClazzName.equals("StreamingFileWriter")) {
        org.apache.flink.table.types.logical.RowType inputType =
            (org.apache.flink.table.types.logical.RowType)
                ((InternalTypeInfo) transformation.getInputType()).toLogicalType();
        RowType inputDataColumns = (RowType) LogicalTypeConverter.toVLType(inputType);
        RowType ignore = new RowType(List.of("num"), List.of(new BigIntType()));
        OneInputStreamOperator<IN, OUT> operator = transformation.getOperator();
        List<Integer> partitionIndexes = fileWriterPartitionIndexes(operator);
        List<String> partitionKeys = fileWriterPartitionKeys(operator);
        Map<String, String> tableParams = fileWriterTableParameters(operator);
        FileSystemInsertTableHandle insertTableHandle =
            new FileSystemInsertTableHandle(
                transformation.getName(),
                inputDataColumns,
                partitionKeys,
                partitionIndexes,
                tableParams);
        TableWriteNode fileSystemWriteNode =
            new TableWriteNode(
                PlanNodeIdGenerator.newId(),
                inputDataColumns,
                inputDataColumns.getNames(),
                null,
                "connector-filesystem",
                insertTableHandle,
                false,
                ignore,
                CommitStrategy.NO_COMMIT,
                List.of(new EmptyNode(inputDataColumns)));
        return translateInternal(
            transformation,
            new GlutenOneInputOperatorFactory(
                new GlutenSingleInputOperator(
                    new StatefulPlanNode(fileSystemWriteNode.getId(), fileSystemWriteNode),
                    PlanNodeIdGenerator.newId(),
                    inputDataColumns,
                    Map.of(fileSystemWriteNode.getId(), ignore))),
            transformation.getInputType(),
            transformation.getStateKeySelector(),
            transformation.getStateKeyType(),
            context);
      } else if (operatorClazzName.equals("PartitionCommitter")) {
        return Collections.emptyList();
      } else {
        return translateInternal(
            transformation,
            transformation.getOperatorFactory(),
            transformation.getInputType(),
            transformation.getStateKeySelector(),
            transformation.getStateKeyType(),
            context);
      }
    } else {
      return translateInternal(
          transformation,
          transformation.getOperatorFactory(),
          transformation.getInputType(),
          transformation.getStateKeySelector(),
          transformation.getStateKeyType(),
          context);
    }
  }
}
