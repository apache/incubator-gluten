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

import io.github.zhztheplayer.velox4j.data.RowVector;
import io.github.zhztheplayer.velox4j.session.Session;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import org.apache.arrow.memory.BufferAllocator;

import java.io.Serializable;

// This bridge is used to convert the input data to RowVector.
public class VectorInputBridge<IN> implements Serializable {
  private static final long serialVersionUID = 1L;
  private final Class<IN> inClass;

  public VectorInputBridge(Class<IN> inClass) {
    this.inClass = inClass;
  }

  public RowVector getRowVector(
      StreamRecord<IN> inputData, BufferAllocator allocator, Session session, RowType inputType) {
    if (inClass.isAssignableFrom(RowData.class)) {
      RowData rowData = (RowData) inputData.getValue();
      return FlinkRowToVLVectorConvertor.fromRowData(rowData, allocator, session, inputType);
    } else if (inClass.isAssignableFrom(StatefulRecord.class)) {
      // Create a new RowVector Reference. And the original RowVector Object is safe to close.
      return ((StatefulRecord) inputData.getValue()).getRowVector();
    } else {
      throw new UnsupportedOperationException("Unsupported input class: " + inClass.getName());
    }
  }
}
