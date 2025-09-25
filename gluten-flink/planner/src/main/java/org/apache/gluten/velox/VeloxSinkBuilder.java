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
package org.apache.gluten.velox;

import org.apache.gluten.streaming.api.operators.GlutenOneInputOperatorFactory;
import org.apache.gluten.table.runtime.operators.GlutenVectorOneInputOperator;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;
import org.apache.gluten.util.ReflectUtils;

import io.github.zhztheplayer.velox4j.connector.CommitStrategy;
import io.github.zhztheplayer.velox4j.connector.FileSystemInsertTableHandle;
import io.github.zhztheplayer.velox4j.connector.PrintTableHandle;
import io.github.zhztheplayer.velox4j.plan.EmptyNode;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.TableWriteNode;
import io.github.zhztheplayer.velox4j.type.BigIntType;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.runtime.operators.sink.SinkOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class VeloxSinkBuilder {

  public static Transformation<?> build(
      Transformation<?> transformation, ReadableConfig config, ResolvedSchema schema) {
    if (transformation instanceof LegacySinkTransformation) {
      SimpleOperatorFactory<?> operatorFactory =
          (SimpleOperatorFactory<?>)
              ((LegacySinkTransformation<?>) transformation).getOperatorFactory();
      OneInputStreamOperator<?, ?> sinkOp =
          (OneInputStreamOperator<?, ?>) operatorFactory.getOperator();
      if (sinkOp instanceof SinkOperator
          && ((SinkOperator) sinkOp)
              .getUserFunction()
              .getClass()
              .getSimpleName()
              .equals("RowDataPrintFunction")) {
        return buildPrintSink((LegacySinkTransformation<?>) transformation, config);
      }
    } else if (transformation instanceof OneInputTransformation) {
      OneInputTransformation<?, ?> oneInput = (OneInputTransformation<?, ?>) transformation;
      Transformation<?> preInput = (Transformation<?>) oneInput.getInputs().get(0);
      if (oneInput.getName().equals("PartitionCommitter")
          && preInput.getName().equals("StreamingFileWriter")) {
        try {
          return buildFileSystemSink((OneInputTransformation<?, ?>) preInput, config, schema);
        } catch (Exception e) {
          throw new FlinkRuntimeException(e);
        }
      }
    }
    return transformation;
  }

  @SuppressWarnings({"unchecked"})
  private static Transformation<?> buildFileSystemSink(
      OneInputTransformation<?, ?> transformation, ReadableConfig config, ResolvedSchema schema)
      throws Exception {
    OneInputStreamOperator<?, ?> operator = transformation.getOperator();
    List<String> partitionKeys =
        (List<String>) ReflectUtils.getObjectField(operator.getClass(), operator, "partitionKeys");
    List<String> columnList = schema.getColumnNames();
    List<Integer> partitionIndexes =
        partitionKeys.stream().mapToInt(columnList::indexOf).boxed().collect(Collectors.toList());
    Map<String, String> tableParams = config.toMap();
    tableParams.put("fs.file_name_prefix", UUID.randomUUID().toString());
    tableParams.put("fs.writer_task_id", String.valueOf(0));
    org.apache.flink.table.types.logical.RowType inputType =
        (org.apache.flink.table.types.logical.RowType)
            ((InternalTypeInfo<?>) transformation.getInputType()).toLogicalType();
    RowType inputDataColumns = (RowType) LogicalTypeConverter.toVLType(inputType);
    FileSystemInsertTableHandle insertTableHandle =
        new FileSystemInsertTableHandle(
            transformation.getName(),
            inputDataColumns,
            partitionKeys,
            partitionIndexes,
            tableParams);
    RowType ignore = new RowType(List.of("num"), List.of(new BigIntType()));
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
    GlutenVectorOneInputOperator onewInputOperator =
        new GlutenVectorOneInputOperator(
            new StatefulPlanNode(fileSystemWriteNode.getId(), fileSystemWriteNode),
            PlanNodeIdGenerator.newId(),
            inputDataColumns,
            Map.of(fileSystemWriteNode.getId(), ignore));
    GlutenOneInputOperatorFactory<?, ?> operatorFactory =
        new GlutenOneInputOperatorFactory(onewInputOperator);
    return new OneInputTransformation(
        transformation.getInputs().get(0),
        transformation.getName(),
        operatorFactory,
        transformation.getOutputType(),
        transformation.getParallelism());
  }

  @SuppressWarnings({"unchecked"})
  private static LegacySinkTransformation<?> buildPrintSink(
      LegacySinkTransformation<?> transformation, ReadableConfig config) {
    Transformation<?> inputTrans = (Transformation<?>) transformation.getInputs().get(0);
    InternalTypeInfo<?> inputTypeInfo = (InternalTypeInfo<?>) inputTrans.getOutputType();
    String logDir = config.get(CoreOptions.FLINK_LOG_DIR);
    String printPath;
    if (logDir != null) {
      printPath = String.format("file://%s/%s", logDir, "taskmanager.out");
    } else {
      String flinkHomeDir = System.getenv(ConfigConstants.ENV_FLINK_HOME_DIR);
      if (flinkHomeDir == null) {
        String flinkConfDir = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
        if (flinkConfDir == null) {
          throw new FlinkRuntimeException(
              "Can not get flink home directory, please set FLINK_HOME.");
        }
        printPath = String.format("file://%s/../log/%s", flinkConfDir, "taskmanager.out");
      } else {
        printPath = String.format("file://%s/log/%s", flinkHomeDir, "taskmanager.out");
      }
    }
    RowType inputColumns = (RowType) LogicalTypeConverter.toVLType(inputTypeInfo.toLogicalType());
    RowType ignore = new RowType(List.of("num"), List.of(new BigIntType()));
    PrintTableHandle tableHandle = new PrintTableHandle("print-table", inputColumns, printPath);
    TableWriteNode tableWriteNode =
        new TableWriteNode(
            PlanNodeIdGenerator.newId(),
            inputColumns,
            inputColumns.getNames(),
            null,
            "connector-print",
            tableHandle,
            false,
            ignore,
            CommitStrategy.NO_COMMIT,
            List.of(new EmptyNode(inputColumns)));
    GlutenVectorOneInputOperator oneInputOperator =
        new GlutenVectorOneInputOperator(
            new StatefulPlanNode(tableWriteNode.getId(), tableWriteNode),
            PlanNodeIdGenerator.newId(),
            inputColumns,
            Map.of(tableWriteNode.getId(), ignore));
    GlutenOneInputOperatorFactory<?, ?> oneInputOperatorFactory =
        new GlutenOneInputOperatorFactory<>(oneInputOperator);
    return new LegacySinkTransformation(
        inputTrans,
        transformation.getName(),
        oneInputOperatorFactory,
        transformation.getParallelism());
  }
}
