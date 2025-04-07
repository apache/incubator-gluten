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

import io.github.zhztheplayer.velox4j.connector.ExternalStream;
import io.github.zhztheplayer.velox4j.connector.ExternalStreamConnectorSplit;
import io.github.zhztheplayer.velox4j.iterator.CloseableIterator;
import io.github.zhztheplayer.velox4j.iterator.DownIterators;
import io.github.zhztheplayer.velox4j.iterator.UpIterators;
import io.github.zhztheplayer.velox4j.query.BoundSplit;
import io.github.zhztheplayer.velox4j.serde.Serde;
import io.github.zhztheplayer.velox4j.type.RowType;
import org.apache.gluten.streaming.api.operators.GlutenOperator;
import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.session.Session;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/** Calculate operator in gluten, which will call Velox to run. */
public class GlutenCalOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, GlutenOperator {

    private final String glutenPlan;
    private final String id;
    private final String inputType;
    private final String outputType;

    private StreamRecord outElement = null;

    private Session session;
    private Query query;
    private BlockingQueue<RowVector> inputQueue;
    BufferAllocator allocator;

    public GlutenCalOperator(String plan, String id, String inputType, String outputType) {
        this.glutenPlan = plan;
        this.id = id;
        this.inputType = inputType;
        this.outputType = outputType;
    }

    @Override
    public void open() throws Exception {
        super.open();
        outElement = new StreamRecord(null);
        session = Velox4j.newSession(MemoryManager.create(AllocationListener.NOOP));

        inputQueue = new LinkedBlockingQueue<>();
        ExternalStream es =
                session.externalStreamOps().bind(DownIterators.fromBlockingQueue(inputQueue));
        List<BoundSplit> splits = List.of(
                new BoundSplit(
                        id,
                        -1,
                        new ExternalStreamConnectorSplit("connector-external-stream", es.id())));
        PlanNode filter = Serde.fromJson(glutenPlan, PlanNode.class);
        query = new Query(filter, splits, Config.empty(), ConnectorConfig.empty());
        allocator = new RootAllocator(Long.MAX_VALUE);

    }

    @Override
    public void processElement(StreamRecord<RowData> element) {
        inputQueue.add(
                FlinkRowToVLVectorConvertor.fromRowData(
                        element.getValue(),
                        allocator,
                        session,
                        Serde.fromJson(inputType, RowType.class)));
        CloseableIterator<RowVector> result =
                UpIterators.asJavaIterator(session.queryOps().execute(query));
        if (result.hasNext()) {
            List<RowData> rows = FlinkRowToVLVectorConvertor.toRowData(
                    result.next(),
                    allocator,
                    session,
                    Serde.fromJson(outputType, RowType.class));
            for (RowData row : rows) {
                output.collect(outElement.replace(row));
            }
        }
    }

    @Override
    public PlanNode getPlanNode() {
        // TODO: support wartermark operator
        if (glutenPlan == "Watermark") {
            return null;
        }
        return Serde.fromJson(glutenPlan, PlanNode.class);
    }

    @Override
    public RowType getOutputType() {
        return Serde.fromJson(outputType, RowType.class);
    }

    @Override
    public String getId() {
        return id;
    }
}
