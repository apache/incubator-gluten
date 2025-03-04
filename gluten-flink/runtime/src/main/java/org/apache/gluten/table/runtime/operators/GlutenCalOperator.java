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
import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.iterator.DownIterator;
import io.github.zhztheplayer.velox4j.query.BoundSplit;
import org.apache.gluten.vectorized.FlinkRowToVLVectorIterator;
import org.apache.gluten.vectorized.FlinkRowToVLRowConvertor;

import io.github.zhztheplayer.velox4j.Velox4j;
import io.github.zhztheplayer.velox4j.config.Config;
import io.github.zhztheplayer.velox4j.config.ConnectorConfig;
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

/** Calculate operator in gluten, which will call Velox to run. */
public class GlutenCalOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, GlutenOperator {

    private final PlanNode glutenPlan;

    private StreamRecord outElement = null;

    private Session session;
    private Query query;
    private FlinkRowToVLVectorIterator inputIterator;
    BufferAllocator allocator;

    public GlutenCalOperator(PlanNode plan) {
        this.glutenPlan = plan;
    }

    @Override
    public void open() throws Exception {
        super.open();
        outElement = new StreamRecord(null);
        session = Velox4j.newSession(MemoryManager.create(AllocationListener.NOOP));

        inputIterator = new FlinkRowToVLVectorIterator();
        ExternalStream es = session.externalStreamOps().bind(new DownIterator(inputIterator));
        List<BoundSplit> splits = List.of(
                new BoundSplit(
                        "5",
                        -1,
                        new ExternalStreamConnectorSplit("escs1", es.id())));
        query = new Query(glutenPlan, splits, Config.empty(), ConnectorConfig.empty());
        allocator = new RootAllocator(Long.MAX_VALUE);

    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        inputIterator.addRow(
                FlinkRowToVLRowConvertor.fromRowData(
                        element.getValue(),
                        allocator,
                        session));
        RowVector result = session.queryOps().execute(query).next();
        if (result != null) {
            output.collect(
                    outElement.replace(
                            FlinkRowToVLRowConvertor.toRowData(
                                    result,
                                    allocator,
                                    session)));
        }
    }

}
