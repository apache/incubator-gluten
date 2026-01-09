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

import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;

import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

/*
 * This bridge is used to convert the output data to RowData or StatefulRecord.
 * and collect the output data to the collector.
 */
public class VectorOutputBridge<OUT> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(VectorOutputBridge.class);
  private static final long serialVersionUID = 1L;
  private final Class<OUT> outClass;
  private transient StreamRecord<OUT> outElement;

  public VectorOutputBridge(Class<OUT> outClass) {
    this.outClass = outClass;
    this.outElement = new StreamRecord<>(null);
  }

  private StreamRecord<OUT> getOutElement() {
    if (outElement == null) {
      outElement = new StreamRecord<>(null);
    }
    return outElement;
  }

  public void collect(
      Output<StreamRecord<OUT>> collector,
      StatefulRecord record,
      BufferAllocator allocator,
      RowType outputType) {
    if (outClass.isAssignableFrom(RowData.class)) {
      List<RowData> rows =
          FlinkRowToVLVectorConvertor.toRowData(record.getRowVector(), allocator, outputType);
      for (RowData row : rows) {
        collector.collect(getOutElement().replace((OUT) row));
      }
    } else if (outClass.isAssignableFrom(StatefulRecord.class)) {
      collector.collect(getOutElement().replace((OUT) record));
    } else {
      throw new UnsupportedOperationException("Unsupported output class: " + outClass.getName());
    }
  }
}
