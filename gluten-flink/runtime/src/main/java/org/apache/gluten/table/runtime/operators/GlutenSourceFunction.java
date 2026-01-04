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

import org.apache.gluten.table.runtime.config.VeloxConnectorConfig;
import org.apache.gluten.table.runtime.config.VeloxQueryConfig;
import org.apache.gluten.table.runtime.metrics.SourceTaskMetrics;
import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;

import io.github.zhztheplayer.velox4j.connector.ConnectorSplit;
import io.github.zhztheplayer.velox4j.iterator.UpIterator;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.query.SerialTask;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.stateful.StatefulElement;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Gluten legacy source function, call velox plan to execute. It sends RowVector to downstream
 * instead of RowData to avoid data convert.
 */
public class GlutenSourceFunction<OUT> extends RichParallelSourceFunction<OUT>
    implements CheckpointedFunction {
  private static final Logger LOG = LoggerFactory.getLogger(GlutenSourceFunction.class);

  private final StatefulPlanNode planNode;
  private final Map<String, RowType> outputTypes;
  private final String id;
  private final ConnectorSplit split;
  private volatile boolean isRunning = true;

  private GlutenSessionResource sessionResource;
  private Query query;
  private SerialTask task;
  private SourceTaskMetrics taskMetrics;
  private final Class<OUT> outClass;
  private boolean isClosed = false;

  public GlutenSourceFunction(
      StatefulPlanNode planNode,
      Map<String, RowType> outputTypes,
      String id,
      ConnectorSplit split,
      Class<OUT> outClass) {
    this.planNode = planNode;
    this.outputTypes = outputTypes;
    this.id = id;
    this.split = split;
    this.outClass = outClass;
  }

  public StatefulPlanNode getPlanNode() {
    return planNode;
  }

  public Map<String, RowType> getOutputTypes() {
    return outputTypes;
  }

  public String getId() {
    return id;
  }

  public ConnectorSplit getConnectorSplit() {
    return split;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    initSession();
  }

  @Override
  public void run(SourceContext<OUT> sourceContext) throws Exception {

    while (isRunning) {
      UpIterator.State state = task.advance();
      if (state == UpIterator.State.AVAILABLE) {
        StatefulElement element = task.statefulGet();
        if (element.isRecord()) {
          StatefulRecord record = element.asRecord();
          if (outClass.isAssignableFrom(RowData.class)) {
            List<RowData> rows =
                FlinkRowToVLVectorConvertor.toRowData(
                    record.getRowVector(), sessionResource.getAllocator(), outputTypes.get(id));
            for (RowData row : rows) {
              sourceContext.collect((OUT) row);
            }
          } else if (outClass.isAssignableFrom(StatefulRecord.class)) {
            StatefulRecord statefulRecord = (StatefulRecord) record;
            sourceContext.collect((OUT) record);
          } else {
            throw new UnsupportedOperationException(
                "Unsupported output class: " + outClass.getName());
          }
        } else if (element.isWatermark()) {
          sourceContext.emitWatermark(new Watermark(element.asWatermark().getTimestamp()));
        } else {
          LOG.debug("ignore not record or watermark element");
        }
        element.close();
      } else if (state == UpIterator.State.BLOCKED) {
        LOG.debug("Get empty row");
      } else {
        LOG.info("Velox task finished");
        break;
      }
      taskMetrics.updateMetrics(task, id);
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
    throw new RuntimeException("Not implemented for gluten");
  }

  @Override
  public void close() throws Exception {
    isRunning = false;
    if (task != null) {
      task.close();
      task = null;
    }
    if (sessionResource != null) {
      sessionResource.close();
      sessionResource = null;
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    // TODO: implement it
    this.task.snapshotState(0);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    initSession();
    // TODO: implement it
    this.task.initializeState(0);
  }

  public String[] notifyCheckpointComplete(long checkpointId) throws Exception {
    // TODO: notify velox
    return this.task.notifyCheckpointComplete(checkpointId);
  }

  public void notifyCheckpointAborted(long checkpointId) throws Exception {
    // TODO: notify velox
    this.task.notifyCheckpointAborted(checkpointId);
  }

  private void initSession() {
    if (sessionResource != null) {
      return;
    }
    sessionResource = new GlutenSessionResource();
    Session session = sessionResource.getSession();
    query =
        new Query(
            planNode,
            VeloxQueryConfig.getConfig(getRuntimeContext()),
            VeloxConnectorConfig.getConfig(getRuntimeContext()));
    task = session.queryOps().execute(query);
    task.addSplit(id, split);
    task.noMoreSplits(id);
    taskMetrics = new SourceTaskMetrics(getRuntimeContext().getMetricGroup());
  }
}
