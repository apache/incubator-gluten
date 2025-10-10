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

import org.apache.gluten.streaming.api.operators.GlutenOperator;
import org.apache.gluten.table.runtime.config.VeloxQueryConfig;
import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamTableHandle;
import io.github.zhztheplayer.velox4j.connector.ExternalStreams;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.query.SerialTask;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.stateful.StatefulElement;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/** Calculate operator in gluten, which will call Velox to run. */
public class GlutenOneInputOperator extends TableStreamOperator<RowData>
    implements OneInputStreamOperator<RowData, RowData>, GlutenOperator {

  private static final Logger LOG = LoggerFactory.getLogger(GlutenOneInputOperator.class);

  private final StatefulPlanNode glutenPlan;
  private final String id;
  private final RowType inputType;
  private final Map<String, RowType> outputTypes;

  private StreamRecord<RowData> outElement = null;

  private MemoryManager memoryManager;
  private Session session;
  private Query query;
  private ExternalStreams.BlockingQueue inputQueue;
  private BufferAllocator allocator;
  private SerialTask task;

  public GlutenOneInputOperator(
      StatefulPlanNode plan, String id, RowType inputType, Map<String, RowType> outputTypes) {
    this.glutenPlan = plan;
    this.id = id;
    this.inputType = inputType;
    this.outputTypes = outputTypes;
  }

  @Override
  public void open() throws Exception {
    super.open();
    outElement = new StreamRecord(null);
    memoryManager = MemoryManager.create(AllocationListener.NOOP);
    session = Velox4j.newSession(memoryManager);

    inputQueue = session.externalStreamOps().newBlockingQueue();
    // add a mock input as velox not allow the source is empty.
    StatefulPlanNode mockInput =
        new StatefulPlanNode(
            id,
            new TableScanNode(
                id,
                inputType,
                new ExternalStreamTableHandle("connector-external-stream"),
                List.of()));
    mockInput.addTarget(glutenPlan);
    LOG.debug("Gluten Plan: {}", Serde.toJson(mockInput));
    LOG.debug("OutTypes: {}", outputTypes.keySet());
    query =
        new Query(
            mockInput, VeloxQueryConfig.getConfig(getRuntimeContext()), ConnectorConfig.empty());
    allocator = new RootAllocator(Long.MAX_VALUE);
    task = session.queryOps().execute(query);
    ExternalStreamConnectorSplit split =
        new ExternalStreamConnectorSplit("connector-external-stream", inputQueue.id());
    task.addSplit(id, split);
    task.noMoreSplits(id);
  }

  @Override
  public void processElement(StreamRecord<RowData> element) {
    try (RowVector inRv =
        FlinkRowToVLVectorConvertor.fromRowData(
            element.getValue(), allocator, session, inputType)) {
      inputQueue.put(inRv);
      UpIterator.State state = task.advance();
      if (state == UpIterator.State.AVAILABLE) {
        final StatefulElement statefulElement = task.statefulGet();

        try (RowVector outRv = statefulElement.asRecord().getRowVector()) {
          List<RowData> rows =
              FlinkRowToVLVectorConvertor.toRowData(
                  outRv, allocator, outputTypes.values().iterator().next());
          for (RowData row : rows) {
            output.collect(outElement.replace(row));
          }
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    inputQueue.close();
    task.close();
    session.close();
    memoryManager.close();
    allocator.close();
  }

  @Override
  public StatefulPlanNode getPlanNode() {
    return glutenPlan;
  }

  @Override
  public RowType getInputType() {
    return inputType;
  }

  @Override
  public Map<String, RowType> getOutputTypes() {
    return outputTypes;
  }

  @Override
  public String getId() {
    return id;
  }
}
