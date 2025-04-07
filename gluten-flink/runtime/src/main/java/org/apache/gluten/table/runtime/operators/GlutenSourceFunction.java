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

import io.github.zhztheplayer.velox4j.connector.FuzzerConnectorSplit;
import io.github.zhztheplayer.velox4j.query.BoundSplit;
import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.CloseableIterator;
import io.github.zhztheplayer.velox4j.iterator.UpIterators;
import io.github.zhztheplayer.velox4j.memory.AllocationListener;
import io.github.zhztheplayer.velox4j.memory.MemoryManager;
import io.github.zhztheplayer.velox4j.plan.PlanNode;
import io.github.zhztheplayer.velox4j.query.Query;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.type.RowType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;

import java.util.List;

/** Gluten legacy source function, call velox plan to execute. */
public class GlutenSourceFunction extends RichParallelSourceFunction<RowData> {

    private final PlanNode planNode;
    private final RowType outputType;
    private final String id;
    private volatile boolean isRunning = true;

    private Session session;
    private Query query;
    BufferAllocator allocator;

    public GlutenSourceFunction(PlanNode planNode, RowType outputType, String id) {
        this.planNode = planNode;
        this.outputType = outputType;
        this.id = id;
    }

    public PlanNode getPlanNode() {
        return planNode;
    }

    public RowType getOutputType() { return outputType; }

    public String getId() { return id; }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
        final List<BoundSplit> splits = List.of(new BoundSplit(
                id,
                -1,
                new FuzzerConnectorSplit("connector-fuzzer", 1000)));
        session = Velox4j.newSession(MemoryManager.create(AllocationListener.NOOP));
        query = new Query(planNode, splits, Config.empty(), ConnectorConfig.empty());
        allocator = new RootAllocator(Long.MAX_VALUE);

        while (isRunning) {
            CloseableIterator<RowVector> result =
                    UpIterators.asJavaIterator(session.queryOps().execute(query));
            if (result.hasNext()) {
                List<RowData> rows = FlinkRowToVLVectorConvertor.toRowData(
                        result.next(),
                        allocator,
                        session,
                        outputType);
                for (RowData row : rows) {
                    sourceContext.collect(row);
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
