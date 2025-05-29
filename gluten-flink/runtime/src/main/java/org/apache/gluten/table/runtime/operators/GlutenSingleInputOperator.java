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
import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamTableHandle;
import io.github.zhztheplayer.velox4j.connector.ExternalStreams;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.plan.TableScanNode;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.query.SerialTask;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.session.Session;
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

/** Calculate operator in gluten, which will call Velox to run. */
public class GlutenSingleInputOperator extends TableStreamOperator<RowData>
    implements OneInputStreamOperator<RowData, RowData>, GlutenOperator {

  private static final Logger LOG = LoggerFactory.getLogger(GlutenSingleInputOperator.class);

  private final PlanNode glutenPlan;
  private final String id;
  private final RowType inputType;
  private final RowType outputType;

  private StreamRecord<RowData> outElement = null;

  private MemoryManager memoryManager;
  private Session session;
  private Query query;
  private ExternalStreams.BlockingQueue inputQueue;
  private BufferAllocator allocator;
  private SerialTask task;

  public GlutenSingleInputOperator(
      PlanNode plan, String id, RowType inputType, RowType outputType) {
    this.glutenPlan = plan;
    this.id = id;
    this.inputType = inputType;
    this.outputType = outputType;
  }

  @Override
  public void open() throws Exception {
    super.open();
    outElement = new StreamRecord(null);
    memoryManager = MemoryManager.create(AllocationListener.NOOP);
    session = Velox4j.newSession(memoryManager);

    inputQueue = session.externalStreamOps().newBlockingQueue();
    // add a mock input as velox not allow the source is empty.
    PlanNode mockInput =
        new TableScanNode(
            id, inputType, new ExternalStreamTableHandle("connector-external-stream"), List.of());
    glutenPlan.setSources(List.of(mockInput));
    LOG.debug("Gluten Plan: {}", Serde.toJson(glutenPlan));
    query = new Query(glutenPlan, Config.empty(), ConnectorConfig.empty());
    allocator = new RootAllocator(Long.MAX_VALUE);
    task = session.queryOps().execute(query);
    ExternalStreamConnectorSplit split =
        new ExternalStreamConnectorSplit("connector-external-stream", inputQueue.id());
    task.addSplit(id, split);
    task.noMoreSplits(id);
  }

  @Override
  public void processElement(StreamRecord<RowData> element) {
    RowVector inRv = null;
    RowVector outRv = null;
    try {
      inRv =
          FlinkRowToVLVectorConvertor.fromRowData(
              element.getValue(), allocator, session, inputType);
      inputQueue.put(inRv);
      UpIterator.State state = task.advance();
      if (state == UpIterator.State.AVAILABLE) {
        outRv = task.get();
        List<RowData> rows = FlinkRowToVLVectorConvertor.toRowData(outRv, allocator, outputType);
        for (RowData row : rows) {
          output.collect(outElement.replace(row));
        }
      }
    } finally {
      /// The RowVector should be closed in `finally`, to avoid it may not be closed when exceptions
      // rasied,
      /// that lead to memory leak.
      if (outRv != null) {
        outRv.close();
      }
      if (inRv != null) {
        inRv.close();
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
  public PlanNode getPlanNode() {
    return glutenPlan;
  }

  @Override
  public RowType getInputType() {
    return inputType;
  }

  @Override
  public RowType getOutputType() {
    return outputType;
  }

  @Override
  public String getId() {
    return id;
  }
}
