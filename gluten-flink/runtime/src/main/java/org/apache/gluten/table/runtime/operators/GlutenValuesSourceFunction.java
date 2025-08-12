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
package org.apache.gluten.table.runtime.operators;

import org.apache.gluten.util.FlinkRowToRowDataConverter;
import org.apache.gluten.util.LogicalTypeConverter;
import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.connector.ConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.VectorConnectorSplit;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.query.SerialTask;
import io.github.zhztheplayer.velox4j.stateful.StatefulElement;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import org.apache.arrow.memory.RootAllocator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GlutenValuesSourceFunction extends GlutenVectorSourceFunction {

  private List<Row> values;
  private org.apache.flink.table.types.logical.RowType valueType;

  public GlutenValuesSourceFunction(
      StatefulPlanNode planNode,
      Map<String, RowType> outputTypes,
      String id,
      ConnectorSplit split,
      org.apache.flink.table.types.logical.RowType valueType,
      List<Row> values) {
    super(planNode, outputTypes, id, split);
    this.values = values;
    this.valueType = valueType;
  }

  public List<Row> getValues() {
    return values;
  }

  public org.apache.flink.table.types.logical.RowType getValueType() {
    return valueType;
  }

  @Override
  public void open(OpenContext context) throws Exception {
    super.open(context);
    memoryManager = MemoryManager.create(AllocationListener.NOOP);
    session = Velox4j.newSession(memoryManager);
    query = new Query(getPlanNode(), Config.empty(), ConnectorConfig.empty());
    allocator = new RootAllocator(Long.MAX_VALUE);
    List<RowVector> vectors = new ArrayList<>();
    String data = "";
    if (values != null) {
      RowRowConverter rowConverter =
          FlinkRowToRowDataConverter.create(TypeConversions.fromLogicalToDataType(valueType));
      RowType vlRowType = (RowType) LogicalTypeConverter.toVLType(valueType);
      for (Row row : values) {
        RowData rowData = rowConverter.toInternal(row);
        RowVector vec =
            FlinkRowToVLVectorConvertor.fromRowData(rowData, allocator, session, vlRowType);
        vectors.add(vec);
      }
    }

    if (vectors.size() > 0) {
      RowVector rowVector =
          session.baseVectorOps().createEmpty(vectors.get(0).getType()).asRowVector();
      for (int i = 0; i < vectors.size(); ++i) {
        rowVector.append(vectors.get(i));
      }
      data = rowVector.serialize();
      for (RowVector vec : vectors) {
        vec.close();
      }
      rowVector.close();
    }
    split = new VectorConnectorSplit("connector-vector", 0, false, data);
  }

  @Override
  public void run(SourceContext<StatefulElement> sourceContext) throws Exception {
    SerialTask task = session.queryOps().execute(query);
    task.addSplit(getId(), split);
    task.noMoreSplits(getId());
    UpIterator.State state = task.advance();
    if (state == UpIterator.State.AVAILABLE) {
      final StatefulElement element = task.statefulGet();
      if (element.isWatermark()) {
        sourceContext.emitWatermark(new Watermark(element.asWatermark().getTimestamp()));
      } else {
        sourceContext.collect(element);
      }
      element.close();
    }
    task.close();
  }

  @Override
  public void close() {
    session.close();
    memoryManager.close();
    allocator.close();
  }
}
