/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators;

import org.apache.flink.streaming.api.operators.GlutenOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import io.substrait.proto.Plan;
import org.apache.gluten.backendsapi.FlinkBackend;
import org.apache.gluten.vectorized.VLNativeRowVector;

/** Calculate operator in gluten, which will call Velox to run. */
public class GlutenCalOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, GlutenOperator {

    private final byte[] glutenPlan;

    private StreamRecord outElement = null;

    private int nativeExecutor;

    public GlutenCalOperator(Plan plan) {
        this.glutenPlan = plan.toByteArray();
    }

    @Override
    public void open() throws Exception {
        super.open();
        outElement = new StreamRecord(null);

        nativeExecutor = FlinkBackend.flinkPlanExecApi().generateOperator(glutenPlan);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        // TODO: use velox jni methods to run?
        long res = nativeProcessElement(
            nativeExecutor,
            VLNativeRowVector.fromRowData(element.getValue()).rowAddress());
        if (res > 0) {
            output.collect(outElement.replace(new VLNativeRowVector(res).toRowData()));
        }
    }

    private native long nativeProcessElement(int executor, long data);
}
