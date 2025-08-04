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
package org.apache.gluten.table.runtime.rowdata;

import org.apache.gluten.vectorized.FlinkRowToVLVectorConvertor;

import io.github.zhztheplayer.velox4j.stateful.StatefulElement;
import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.arrow.memory.BufferAllocator;

import java.util.List;

public class GlutenStatefulRowData implements RowData {

  private StatefulElement element;
  private RowKind kind;
  private BufferAllocator allocator;
  private RowData internalRow = null;

  public GlutenStatefulRowData(StatefulElement element, RowKind kind, BufferAllocator allocator) {
    this.element = element;
    this.kind = kind;
    this.allocator = allocator;
  }

  public GlutenStatefulRowData(StatefulElement element, BufferAllocator allocator) {
    this.element = element;
    this.kind = RowKind.INSERT;
    this.allocator = allocator;
  }

  public StatefulRecord getRecord() {
    if (element instanceof StatefulRecord) {
      return (StatefulRecord) element;
    } else {
      throw new FlinkRuntimeException("The internal element is not stateful record.");
    }
  }

  public StatefulElement getElement() {
    return element;
  }

  private RowData getInternalRow() {
    if (internalRow != null) {
      return internalRow;
    } else if (element instanceof StatefulRecord) {
      synchronized (element) {
        if (internalRow != null) {
          return internalRow;
        }
        StatefulRecord statefulRecord = (StatefulRecord) element;
        List<RowData> rows =
            FlinkRowToVLVectorConvertor.toRowData(
                statefulRecord.getRowVector(),
                allocator,
                (RowType) statefulRecord.getRowVector().getType());
        internalRow = rows.get(0);
        return internalRow;
      }
    } else {
      String errMsg =
          String.format("can not convert %s to row data type", element.getClass().getName());
      throw new FlinkRuntimeException(errMsg);
    }
  }

  @Override
  public int getArity() {
    if (element instanceof StatefulRecord) {
      StatefulRecord statefulRecord = (StatefulRecord) element;
      io.github.zhztheplayer.velox4j.type.RowType rowType =
          (io.github.zhztheplayer.velox4j.type.RowType) statefulRecord.getRowVector().getType();
      return rowType.getChildren().size();
    } else {
      throw new FlinkRuntimeException("Failed to arity, as the element is not a record");
    }
  }

  @Override
  public RowKind getRowKind() {
    return kind;
  }

  @Override
  public void setRowKind(RowKind kind) {
    this.kind = kind;
  }

  @Override
  public boolean isNullAt(int pos) {
    return getInternalRow().isNullAt(pos);
  }

  @Override
  public boolean getBoolean(int pos) {
    return getInternalRow().getBoolean(pos);
  }

  @Override
  public byte getByte(int pos) {
    return getInternalRow().getByte(pos);
  }

  @Override
  public short getShort(int pos) {
    return getInternalRow().getShort(pos);
  }

  @Override
  public int getInt(int pos) {
    return getInternalRow().getInt(pos);
  }

  @Override
  public long getLong(int pos) {
    return getInternalRow().getLong(pos);
  }

  @Override
  public float getFloat(int pos) {
    return getInternalRow().getFloat(pos);
  }

  @Override
  public double getDouble(int pos) {
    return getInternalRow().getDouble(pos);
  }

  @Override
  public StringData getString(int pos) {
    return getInternalRow().getString(pos);
  }

  @Override
  public DecimalData getDecimal(int pos, int precision, int scale) {
    return getInternalRow().getDecimal(pos, precision, scale);
  }

  @Override
  public TimestampData getTimestamp(int pos, int precision) {
    return getInternalRow().getTimestamp(pos, precision);
  }

  @Override
  public <T> RawValueData<T> getRawValue(int pos) {
    return getInternalRow().getRawValue(pos);
  }

  @Override
  public byte[] getBinary(int pos) {
    return getInternalRow().getBinary(pos);
  }

  @Override
  public ArrayData getArray(int pos) {
    return getInternalRow().getArray(pos);
  }

  @Override
  public MapData getMap(int pos) {
    return getInternalRow().getMap(pos);
  }

  @Override
  public RowData getRow(int pos, int numFields) {
    return getInternalRow().getRow(pos, numFields);
  }
}
