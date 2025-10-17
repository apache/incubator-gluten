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

import org.apache.gluten.streaming.api.operators.GlutenStreamSource;
import org.apache.gluten.table.runtime.operators.GlutenVectorSourceFunction;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.util.PlanNodeIdGenerator;
import org.apache.gluten.util.ReflectUtils;

import io.github.zhztheplayer.velox4j.connector.FromElementsConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.FromElementsTableHandle;
import io.github.zhztheplayer.velox4j.connector.KafkaConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.KafkaTableHandle;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class VeloxSourceBuilder {

  public static Transformation<RowData> build(
      Transformation<RowData> transformation,
      ScanTableSource scanTableSource,
      boolean checkpointEnabled) {
    if (transformation instanceof LegacySourceTransformation) {
      if (scanTableSource.getClass().getSimpleName().equals("TestValuesScanLookupTableSource")) {
        return buildFromElementsSource(transformation, scanTableSource);
      }
    } else if (transformation instanceof SourceTransformation) {
      if (scanTableSource.getClass().getSimpleName().equals("KafkaDynamicSource")) {
        return buildFromKafkaSource(transformation, scanTableSource, checkpointEnabled);
      }
    }
    return transformation;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Transformation<RowData> buildFromKafkaSource(
      Transformation<RowData> transformation,
      ScanTableSource tableSource,
      boolean checkpointEnabled) {
    RowType outputType =
        (RowType)
            LogicalTypeConverter.toVLType(
                ((InternalTypeInfo<?>) transformation.getOutputType()).toLogicalType());
    try {
      Class<?> tableSourceClazz =
          Class.forName("org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicSource");
      Properties properties =
          (Properties) ReflectUtils.getObjectField(tableSourceClazz, tableSource, "properties");
      List<String> topics =
          (List<String>) ReflectUtils.getObjectField(tableSourceClazz, tableSource, "topics");
      DecodingFormat decodingFormat =
          (DecodingFormat)
              ReflectUtils.getObjectField(tableSourceClazz, tableSource, "valueDecodingFormat");
      String startupMode =
          String.valueOf(ReflectUtils.getObjectField(tableSourceClazz, tableSource, "startupMode"));
      String connectorId = "connector-kafka";
      String planId = PlanNodeIdGenerator.newId();
      String topic = topics.get(0);
      String format =
          decodingFormat.getClass().getName().contains("JsonFormatFactory") ? "json" : "raw";
      Map<String, String> kafkaTableParameters = new HashMap<String, String>();
      for (String key : properties.stringPropertyNames()) {
        kafkaTableParameters.put(key, properties.getProperty(key));
      }
      kafkaTableParameters.put("topic", topic);
      kafkaTableParameters.put("format", format);
      kafkaTableParameters.put(
          "scan.startup.mode",
          startupMode.equals("LATEST")
              ? "latest-offsets"
              : startupMode.equals("EARLIEST") ? "earliest-offsets" : "group-offsets");
      kafkaTableParameters.put("enable.auto.commit", checkpointEnabled ? "false" : "true");
      kafkaTableParameters.put(
          "client.id",
          properties.getProperty("client.id.prefix", connectorId) + "-" + UUID.randomUUID());
      KafkaTableHandle kafkaTableHandle =
          new KafkaTableHandle(connectorId, topic, outputType, kafkaTableParameters);
      KafkaConnectorSplit connectorSplit =
          new KafkaConnectorSplit(
              connectorId,
              0,
              false,
              kafkaTableParameters.get("bootstrap.servers"),
              kafkaTableParameters.get("group.id"),
              format,
              Boolean.valueOf(kafkaTableParameters.getOrDefault("enable.auto.commit", "false")),
              "latest",
              List.of());
      TableScanNode kafkaScan = new TableScanNode(planId, outputType, kafkaTableHandle, List.of());
      GlutenStreamSource sourceOp =
          new GlutenStreamSource(
              new GlutenVectorSourceFunction(
                  new StatefulPlanNode(kafkaScan.getId(), kafkaScan),
                  Map.of(kafkaScan.getId(), outputType),
                  kafkaScan.getId(),
                  connectorSplit));
      SourceTransformation sourceTransformation = (SourceTransformation) transformation;
      return new LegacySourceTransformation<RowData>(
          sourceTransformation.getName(),
          sourceOp,
          transformation.getOutputType(),
          sourceTransformation.getParallelism(),
          sourceTransformation.getBoundedness(),
          false);
    } catch (Exception e) {
      throw new FlinkRuntimeException(e);
    }
  }

  /** `FromElementsSource` is designed for ut tests, and we map it to velox source. */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Transformation<RowData> buildFromElementsSource(
      Transformation<RowData> transformation, ScanTableSource tableSource) {
    LegacySourceTransformation<RowData> sourceTransformation =
        (LegacySourceTransformation<RowData>) transformation;
    try {
      Class<?> tableSourceClazz =
          Class.forName(
              "org.apache.flink.table.planner.factories.TestValuesTableFactory$TestValuesScanTableSourceWithoutProjectionPushDown");
      Map<Map<String, String>, Collection<Row>> data =
          (Map) ReflectUtils.getObjectField(tableSourceClazz, tableSource, "data");
      InternalTypeInfo<RowData> typeInfo =
          (InternalTypeInfo<RowData>) sourceTransformation.getOutputType();
      io.github.zhztheplayer.velox4j.type.RowType rowType =
          (io.github.zhztheplayer.velox4j.type.RowType)
              LogicalTypeConverter.toVLType(typeInfo.toLogicalType());
      List<String> values = new ArrayList<>();
      for (Collection<Row> rows : data.values()) {
        for (Row row : rows) {
          Row projectedRow =
              (Row)
                  ReflectUtils.invokeObjectMethod(
                      tableSourceClazz,
                      tableSource,
                      "projectRow",
                      new Class<?>[] {Row.class},
                      new Object[] {row});
          values.add(projectedRow.toString());
        }
      }
      FromElementsTableHandle tableHandle =
          new FromElementsTableHandle(
              "connector-from-elements", "from-elements-table", rowType, values);
      TableScanNode scanNode =
          new TableScanNode(PlanNodeIdGenerator.newId(), rowType, tableHandle, List.of());
      GlutenStreamSource op =
          new GlutenStreamSource(
              new GlutenVectorSourceFunction(
                  new StatefulPlanNode(scanNode.getId(), scanNode),
                  Map.of(scanNode.getId(), rowType),
                  scanNode.getId(),
                  new FromElementsConnectorSplit("connector-from-elements", 0, false)));
      return new LegacySourceTransformation<RowData>(
          sourceTransformation.getName(),
          op,
          typeInfo,
          sourceTransformation.getParallelism(),
          sourceTransformation.getBoundedness(),
          false);
    } catch (Exception e) {
      throw new FlinkRuntimeException(e);
    }
  }
}
