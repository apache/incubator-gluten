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

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamConnectorSplit;
import io.github.zhztheplayer.velox4j.connector.ExternalStreams;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.query.SerialTask;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.stateful.StatefulElement;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.stateful.StatefulWatermark;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Two input operator in gluten, which will call Velox to run. It receives RowVector from upstream
 * instead of flink RowData.
 */
public class GlutenVectorTwoInputOperator extends AbstractStreamOperator<StatefulRecord>
    implements TwoInputStreamOperator<StatefulRecord, StatefulRecord, StatefulRecord>,
        GlutenOperator {

  private static final Logger LOG = LoggerFactory.getLogger(GlutenVectorTwoInputOperator.class);

  private final StatefulPlanNode glutenPlan;
  private final String leftId;
  private final String rightId;
  private final RowType leftInputType;
  private final RowType rightInputType;
  private final Map<String, RowType> outputTypes;

  private StreamRecord<StatefulRecord> outElement = null;

  private MemoryManager memoryManager;
  private Session session;
  private Query query;
  private ExternalStreams.BlockingQueue leftInputQueue;
  private ExternalStreams.BlockingQueue rightInputQueue;
  private SerialTask task;

  public GlutenVectorTwoInputOperator(
      StatefulPlanNode plan,
      String leftId,
      String rightId,
      RowType leftInputType,
      RowType rightInputType,
      Map<String, RowType> outputTypes) {
    this.glutenPlan = plan;
    this.leftId = leftId;
    this.rightId = rightId;
    this.leftInputType = leftInputType;
    this.rightInputType = rightInputType;
    this.outputTypes = outputTypes;
  }

  // initializeState is called before open, so need to init gluten task first.
  private void initGlutenTask() {
    memoryManager = MemoryManager.create(AllocationListener.NOOP);
    session = Velox4j.newSession(memoryManager);
    query =
        new Query(
            glutenPlan, VeloxQueryConfig.getConfig(getRuntimeContext()), ConnectorConfig.empty());
    task = session.queryOps().execute(query);
    LOG.debug("Gluten Plan: {}", Serde.toJson(glutenPlan));
    LOG.debug("OutTypes: {}", outputTypes.keySet());
    LOG.debug("RuntimeContext: {}", getRuntimeContext().getClass().getName());
  }

  @Override
  public void open() throws Exception {
    super.open();
    if (task == null) {
      initGlutenTask();
    }
    outElement = new StreamRecord(null);
    leftInputQueue = session.externalStreamOps().newBlockingQueue();
    rightInputQueue = session.externalStreamOps().newBlockingQueue();
    ExternalStreamConnectorSplit leftSplit =
        new ExternalStreamConnectorSplit("connector-external-stream", leftInputQueue.id());
    ExternalStreamConnectorSplit rightSplit =
        new ExternalStreamConnectorSplit("connector-external-stream", rightInputQueue.id());
    task.addSplit(leftId, leftSplit);
    task.noMoreSplits(leftId);
    task.addSplit(rightId, rightSplit);
    task.noMoreSplits(rightId);
  }

  @Override
  public void processElement1(StreamRecord<StatefulRecord> element) {
    final RowVector inRv = element.getValue().getRowVector();
    leftInputQueue.put(inRv);
    processElement();
    inRv.close();
  }

  @Override
  public void processElement2(StreamRecord<StatefulRecord> element) {
    final RowVector inRv = element.getValue().getRowVector();
    rightInputQueue.put(inRv);
    processElement();
    inRv.close();
  }

  private void processElement() {
    while (true) {
      UpIterator.State state = task.advance();
      if (state == UpIterator.State.AVAILABLE) {
        final StatefulElement element = task.statefulGet();
        if (element.isWatermark()) {
          StatefulWatermark watermark = element.asWatermark();
          output.emitWatermark(new Watermark(watermark.getTimestamp()));
        } else {
          final StatefulRecord statefulRecord = element.asRecord();
          output.collect(outElement.replace(statefulRecord));
          statefulRecord.close();
        }
      } else {
        break;
      }
    }
  }

  @Override
  public void processWatermark1(Watermark mark) throws Exception {
    // TODO: implement it;
    task.notifyWatermark(mark.getTimestamp(), 1);
    processElement();
  }

  @Override
  public void processWatermark2(Watermark mark) throws Exception {
    // TODO: implement it;
    task.notifyWatermark(mark.getTimestamp(), 2);
    processElement();
  }

  @Override
  public void close() throws Exception {
    leftInputQueue.close();
    rightInputQueue.close();
    task.close();
    session.close();
    memoryManager.close();
  }

  @Override
  public StatefulPlanNode getPlanNode() {
    return glutenPlan;
  }

  @Override
  public RowType getInputType() {
    throw new RuntimeException("Should not call getInputType on GlutenVectorTwoInputOperator");
  }

  public RowType getLeftInputType() {
    return leftInputType;
  }

  public RowType getRightInputType() {
    return rightInputType;
  }

  @Override
  public Map<String, RowType> getOutputTypes() {
    return outputTypes;
  }

  @Override
  public String getId() {
    throw new RuntimeException("Should not call getId on GlutenVectorTwoInputOperator");
  }

  public String getLeftId() {
    return leftId;
  }

  public String getRightId() {
    return rightId;
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    // TODO: notify velox
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    // TODO: implement it
    task.snapshotState(0);
    super.snapshotState(context);
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    if (task == null) {
      initGlutenTask();
    }
    // TODO: implement it
    task.initializeState(0);
    super.initializeState(context);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    // TODO: notify velox
    task.notifyCheckpointComplete(checkpointId);
    super.notifyCheckpointComplete(checkpointId);
  }

  @Override
  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    // TODO: notify velox
    task.notifyCheckpointAborted(checkpointId);
    super.notifyCheckpointAborted(checkpointId);
  }
}
