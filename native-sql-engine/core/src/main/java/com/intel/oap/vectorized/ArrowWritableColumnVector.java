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

package com.intel.oap.vectorized;

import java.lang.*;
import java.math.BigDecimal;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.*;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.Field;

import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils;
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkSchemaUtils;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.util.ArrowUtils;
import org.apache.spark.sql.vectorized.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A column backed by an in memory JVM array. This stores the NULLs as a byte per value
 * and a java array for the values.
 */
public final class ArrowWritableColumnVector extends WritableColumnVector {
  private static final boolean bigEndianPlatform =
      ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);

  private static final Logger LOG =
      LoggerFactory.getLogger(ArrowWritableColumnVector.class);

  private ArrowVectorAccessor accessor;
  private ArrowVectorWriter writer;

  private int ordinal;
  private ValueVector vector;
  private ValueVector dictionaryVector;
  private static BufferAllocator OffRecordAllocator = SparkMemoryUtils.globalAllocator();

  public static BufferAllocator getAllocator() {
    return SparkMemoryUtils.contextAllocator();
  }
  public static BufferAllocator getOffRecordAllocator() {
    return OffRecordAllocator;
  }
  public static AtomicLong vectorCount = new AtomicLong(0);
  private AtomicLong refCnt = new AtomicLong(0);
  private boolean closed = false;

  /**
   * Allocates columns to store elements of each field of the schema on heap.
   * Capacity is the initial capacity of the vector and it will grow as necessary.
   * Capacity is in number of elements, not number of bytes.
   */
  public static ArrowWritableColumnVector[] allocateColumns(
      int capacity, StructType schema) {
    String timeZoneId = SparkSchemaUtils.getLocalTimezoneID();
    Schema arrowSchema = ArrowUtils.toArrowSchema(schema, timeZoneId);
    VectorSchemaRoot new_root =
        VectorSchemaRoot.create(arrowSchema, SparkMemoryUtils.contextAllocator());

    List<FieldVector> fieldVectors = new_root.getFieldVectors();
    ArrowWritableColumnVector[] vectors =
        new ArrowWritableColumnVector[fieldVectors.size()];
    for (int i = 0; i < fieldVectors.size(); i++) {
      vectors[i] = new ArrowWritableColumnVector(fieldVectors.get(i), i, capacity, true);
    }
    // LOG.info("allocateColumns allocator is " + allocator);
    return vectors;
  }

  public static ArrowWritableColumnVector[] loadColumns(
      int capacity, List<FieldVector> fieldVectors, List<FieldVector> dictionaryVectors) {
    if (fieldVectors.size() != dictionaryVectors.size()) {
      throw new IllegalArgumentException(
          "Mismatched field vectors and dictionary vectors. "
          + "Field vector count: " + fieldVectors.size() + ", "
          + "dictionary vector count: " + dictionaryVectors.size());
    }
    ArrowWritableColumnVector[] vectors =
        new ArrowWritableColumnVector[fieldVectors.size()];
    for (int i = 0; i < fieldVectors.size(); i++) {
      vectors[i] = new ArrowWritableColumnVector(
          fieldVectors.get(i), dictionaryVectors.get(i), i, capacity, false);
    }
    return vectors;
  }

  public static ArrowWritableColumnVector[] loadColumns(
      int capacity, List<FieldVector> fieldVectors) {
    ArrowWritableColumnVector[] vectors =
        new ArrowWritableColumnVector[fieldVectors.size()];
    for (int i = 0; i < fieldVectors.size(); i++) {
      vectors[i] = new ArrowWritableColumnVector(fieldVectors.get(i), i, capacity, false);
    }
    return vectors;
  }

  public static ArrowWritableColumnVector[] loadColumns(
      int capacity, Schema arrowSchema, ArrowRecordBatch recordBatch) {
    return loadColumns(capacity, arrowSchema, recordBatch, null);
  }

  public static ArrowWritableColumnVector[] loadColumns(int capacity, Schema arrowSchema,
      ArrowRecordBatch recordBatch, BufferAllocator _allocator) {
    if (_allocator == null) {
      _allocator = SparkMemoryUtils.contextAllocator();
    }
    VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, _allocator);
    VectorLoader loader = new VectorLoader(root);
    loader.load(recordBatch);
    return loadColumns(capacity, root.getFieldVectors());
  }

  @Deprecated
  public ArrowWritableColumnVector(
      ValueVector vector, int ordinal, int capacity, boolean init) {
    this(vector, null, ordinal, capacity, init);
  }

  public ArrowWritableColumnVector(ValueVector vector, ValueVector dicionaryVector,
      int ordinal, int capacity, boolean init) {
    super(capacity, ArrowUtils.fromArrowField(vector.getField()));
    vectorCount.getAndIncrement();
    refCnt.getAndIncrement();

    this.ordinal = ordinal;
    this.vector = vector;
    this.dictionaryVector = dicionaryVector;
    if (init) {
      vector.setInitialCapacity(capacity);
      vector.allocateNew();
    }
    writer = createVectorWriter(vector);
    createVectorAccessor(vector, dicionaryVector);
  }

  public ArrowWritableColumnVector(int capacity, DataType dataType) {
    super(capacity, dataType);
    vectorCount.getAndIncrement();
    refCnt.getAndIncrement();
    String timeZoneId = SparkSchemaUtils.getLocalTimezoneID();
    List<Field> fields =
        Arrays.asList(ArrowUtils.toArrowField("col", dataType, true, timeZoneId));
    Schema arrowSchema = new Schema(fields);
    VectorSchemaRoot root =
        VectorSchemaRoot.create(arrowSchema, SparkMemoryUtils.contextAllocator());

    List<FieldVector> fieldVectors = root.getFieldVectors();
    vector = fieldVectors.get(0);
    vector.setInitialCapacity(capacity);
    vector.allocateNew();
    this.writer = createVectorWriter(vector);
    createVectorAccessor(vector, null);
  }

  public ValueVector getValueVector() {
    return vector;
  }

  public void setValueCount(int numRows) {
    vector.setValueCount(numRows);
  }

  private void createVectorAccessor(ValueVector vector, ValueVector dictionary) {
    if (dictionary != null) {
      if (!(vector instanceof IntVector)) {
        throw new IllegalArgumentException(
            "Expect int32 index vector. Found: " + vector.getMinorType());
      }
      IntVector index = (IntVector) vector;
      if (dictionary instanceof VarBinaryVector) {
        accessor =
            new DictionaryEncodedBinaryAccessor(index, (VarBinaryVector) dictionary);
      } else if (dictionary instanceof VarCharVector) {
        accessor = new DictionaryEncodedStringAccessor(index, (VarCharVector) dictionary);
      } else {
        throw new IllegalArgumentException(
            "Unrecognized index value type: " + dictionary.getMinorType());
      }
      return;
    }
    if (vector instanceof BitVector) {
      accessor = new BooleanAccessor((BitVector) vector);
    } else if (vector instanceof TinyIntVector) {
      accessor = new ByteAccessor((TinyIntVector) vector);
    } else if (vector instanceof SmallIntVector) {
      accessor = new ShortAccessor((SmallIntVector) vector);
    } else if (vector instanceof IntVector) {
      accessor = new IntAccessor((IntVector) vector);
    } else if (vector instanceof BigIntVector) {
      accessor = new LongAccessor((BigIntVector) vector);
    } else if (vector instanceof Float4Vector) {
      accessor = new FloatAccessor((Float4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      accessor = new DoubleAccessor((Float8Vector) vector);
    } else if (vector instanceof DecimalVector) {
      accessor = new DecimalAccessor((DecimalVector) vector);
    } else if (vector instanceof VarCharVector) {
      accessor = new StringAccessor((VarCharVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      accessor = new BinaryAccessor((VarBinaryVector) vector);
    } else if (vector instanceof DateDayVector) {
      accessor = new DateAccessor((DateDayVector) vector);
    } else if (vector instanceof TimeStampMicroVector
        || vector instanceof TimeStampMicroTZVector) {
      accessor = new TimestampMicroAccessor((TimeStampVector) vector);
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      accessor = new ArrayAccessor(listVector);
      childColumns = new ArrowWritableColumnVector[1];
      childColumns[0] = new ArrowWritableColumnVector(
          listVector.getDataVector(), 0, listVector.size(), false);
    } else if (vector instanceof StructVector) {
      throw new UnsupportedOperationException();
      /*StructVector structVector = (StructVector) vector;
      accessor = new StructAccessor(structVector);

      childColumns = new ArrowWritableColumnVector[structVector.size()];
      for (int i = 0; i < childColumns.length; ++i) {
        childColumns[i] = new ArrowWritableColumnVector(structVector.getVectorById(i));
      }*/
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private ArrowVectorWriter createVectorWriter(ValueVector vector) {
    if (vector instanceof BitVector) {
      return new BooleanWriter((BitVector) vector);
    } else if (vector instanceof TinyIntVector) {
      return new ByteWriter((TinyIntVector) vector);
    } else if (vector instanceof SmallIntVector) {
      return new ShortWriter((SmallIntVector) vector);
    } else if (vector instanceof IntVector) {
      return new IntWriter((IntVector) vector);
    } else if (vector instanceof BigIntVector) {
      return new LongWriter((BigIntVector) vector);
    } else if (vector instanceof Float4Vector) {
      return new FloatWriter((Float4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      return new DoubleWriter((Float8Vector) vector);
    } else if (vector instanceof DecimalVector) {
      return new DecimalWriter((DecimalVector) vector);
    } else if (vector instanceof VarCharVector) {
      return new StringWriter((VarCharVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      return new BinaryWriter((VarBinaryVector) vector);
    } else if (vector instanceof DateDayVector) {
      return new DateWriter((DateDayVector) vector);
    } else if (vector instanceof TimeStampMicroVector
        || vector instanceof TimeStampMicroTZVector) {
      return new TimestampMicroWriter((TimeStampVector) vector);
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      ArrowVectorWriter elementVector = createVectorWriter(listVector.getDataVector());
      return new ArrayWriter(listVector, elementVector);
    } else if (vector instanceof StructVector) {
      StructVector structVector = (StructVector) vector;
      ArrowVectorWriter[] children = new ArrowVectorWriter[structVector.size()];
      for (int ordinal = 0; ordinal < structVector.size(); ordinal++) {
        children[ordinal] = createVectorWriter(structVector.getChildByOrdinal(ordinal));
      }
      return new StructWriter(structVector, children);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported data type: " + vector.getMinorType());
    }
  }

  // Spilt this function out since it is the slow path.
  @Override
  protected void reserveInternal(int newCapacity) {
    ValueVector vector = getValueVector();
    int curCapacity = vector.getValueCapacity();
    while (curCapacity < newCapacity) {
      vector.reAlloc();
      curCapacity = vector.getValueCapacity();
    }
  }

  @Override
  protected ArrowWritableColumnVector reserveNewColumn(int capacity, DataType type) {
    return new ArrowWritableColumnVector(capacity, type);
  }

  public void retain() {
    refCnt.getAndIncrement();
  }

  public long refCnt() {
    return vector.getDataBuffer().refCnt();
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    if (refCnt.decrementAndGet() > 0) {
      return;
    }
    closed = true;
    vectorCount.getAndDecrement();
    super.close();
    // TODO: close Arrow Allocated Memory
    if (childColumns != null) {
      for (int i = 0; i < childColumns.length; i++) {
        childColumns[i].close();
        childColumns[i] = null;
      }
      childColumns = null;
    }
    vector.close();
    if (dictionaryVector != null) {
      dictionaryVector.close();
    }
  }

  public static String stat() {
    return "vectorCounter is " + vectorCount.get();
  }

  @Override
  public boolean hasNull() {
    return accessor.getNullCount() > 0;
  }

  @Override
  public int numNulls() {
    return accessor.getNullCount();
  }

  //
  // APIs dealing with general
  //
  public void mergeTo(ArrowWritableColumnVector other, int rowId, int rowNum) {
    for (int i = 0; i < rowNum; i++) {
      if (accessor instanceof BooleanAccessor) {
        if (!isNullAt(i)) {
          other.put(rowId + i, getBoolean(i));
        } else {
          other.putNull(rowId + i);
        }
      } else if (accessor instanceof ByteAccessor) {
        if (!isNullAt(i)) {
          other.put(rowId + i, getByte(i));
        } else {
          other.putNull(rowId + i);
        }
      } else if (accessor instanceof ShortAccessor) {
        if (!isNullAt(i)) {
          other.put(rowId + i, getShort(i));
        } else {
          other.putNull(rowId + i);
        }
      } else if (accessor instanceof IntAccessor) {
        if (!isNullAt(i)) {
          other.put(rowId + i, getInt(i));
        } else {
          other.putNull(rowId + i);
        }
      } else if (accessor instanceof LongAccessor) {
        if (!isNullAt(i)) {
          other.put(rowId + i, getLong(i));
        } else {
          other.putNull(rowId + i);
        }
      } else if (accessor instanceof FloatAccessor) {
        if (!isNullAt(i)) {
          other.put(rowId + i, getFloat(i));
        } else {
          other.putNull(rowId + i);
        }
      } else if (accessor instanceof DoubleAccessor) {
        if (!isNullAt(i)) {
          other.put(rowId + i, getDouble(i));
        } else {
          other.putNull(rowId + i);
        }
      }
    }
  }

  public void put(int rowId, boolean value) {
    putBoolean(rowId, value);
  }

  public void put(int rowId, byte value) {
    putByte(rowId, value);
  }

  public void put(int rowId, short value) {
    putShort(rowId, value);
  }

  public void put(int rowId, int value) {
    putInt(rowId, value);
  }

  public void put(int rowId, long value) {
    putLong(rowId, value);
  }

  public void put(int rowId, float value) {
    putFloat(rowId, value);
  }

  public void put(int rowId, double value) {
    putDouble(rowId, value);
  }

  //
  // APIs dealing with nulls
  //

  @Override
  public void putNotNull(int rowId) {
    writer.setNotNull(rowId);
  }

  @Override
  public void putNull(int rowId) {
    numNulls += 1;
    writer.setNull(rowId);
  }

  @Override
  public void putNulls(int rowId, int count) {
    numNulls += count;
    writer.setNulls(rowId, count);
  }

  @Override
  public void putNotNulls(int rowId, int count) {
    writer.setNotNulls(rowId, count);
  }

  @Override
  public boolean isNullAt(int rowId) {
    return accessor.isNullAt(rowId);
  }

  //
  // APIs dealing with Booleans
  //

  @Override
  public void putBoolean(int rowId, boolean value) {
    writer.setBoolean(rowId, value);
  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {
    writer.setBooleans(rowId, count, value);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return accessor.getBoolean(rowId);
  }

  @Override
  public boolean[] getBooleans(int rowId, int count) {
    return accessor.getBooleans(rowId, count);
  }

  //

  //
  // APIs dealing with Bytes
  //

  @Override
  public void putByte(int rowId, byte value) {
    writer.setByte(rowId, value);
  }

  @Override
  public void putBytes(int rowId, int count, byte value) {
    writer.setBytes(rowId, count, value);
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    writer.setBytes(rowId, count, src, srcIndex);
  }

  public void appendString(byte[] value, int srcIndex, int count) {
    writer.setBytes(elementsAppended, count, value, srcIndex);
    elementsAppended++;
  }

  public void appendDecimal(BigDecimal value) {
    writer.setBytes(elementsAppended, value);
    elementsAppended++;
  }

  @Override
  public byte getByte(int rowId) {
    return accessor.getByte(rowId);
  }

  @Override
  public byte[] getBytes(int rowId, int count) {
    return accessor.getBytes(rowId, count);
  }

  @Override
  protected UTF8String getBytesAsUTF8String(int rowId, int count) {
    return UTF8String.fromBytes(getBytes(rowId, count));
  }

  //
  // APIs dealing with Shorts
  //

  @Override
  public void putShort(int rowId, short value) {
    writer.setShort(rowId, value);
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    writer.setShorts(rowId, count, value);
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    writer.setShorts(rowId, count, src, srcIndex);
  }

  @Override
  public void putShorts(int rowId, int count, byte[] src, int srcIndex) {
    writer.setShorts(rowId, count, src, srcIndex);
  }

  @Override
  public short getShort(int rowId) {
    return accessor.getShort(rowId);
  }

  @Override
  public short[] getShorts(int rowId, int count) {
    return accessor.getShorts(rowId, count);
  }

  //
  // APIs dealing with Ints
  //

  @Override
  public void putInt(int rowId, int value) {
    writer.setInt(rowId, value);
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    writer.setInts(rowId, count, value);
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    writer.setInts(rowId, count, src, srcIndex);
  }

  @Override
  public void putInts(int rowId, int count, byte[] src, int srcIndex) {
    writer.setInts(rowId, count, src, srcIndex);
  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    writer.setIntsLittleEndian(rowId, count, src, srcIndex);
  }

  @Override
  public int getInt(int rowId) {
    return accessor.getInt(rowId);
  }

  @Override
  public int[] getInts(int rowId, int count) {
    return accessor.getInts(rowId, count);
  }

  /**
   * Returns the dictionary Id for rowId.
   * This should only be called when the ColumnVector is dictionaryIds.
   * We have this separate method for dictionaryIds as per SPARK-16928.
   */
  public int getDictId(int rowId) {
    assert (dictionary == null);
    return accessor.getInt(rowId);
  }

  //
  // APIs dealing with Longs
  //

  @Override
  public void putLong(int rowId, long value) {
    writer.setLong(rowId, value);
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    writer.setLongs(rowId, count, value);
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    writer.setLongs(rowId, count, src, srcIndex);
  }

  @Override
  public void putLongs(int rowId, int count, byte[] src, int srcIndex) {
    writer.setLongs(rowId, count, src, srcIndex);
  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
    writer.setLongsLittleEndian(rowId, count, src, srcIndex);
  }

  @Override
  public long getLong(int rowId) {
    return accessor.getLong(rowId);
  }

  @Override
  public long[] getLongs(int rowId, int count) {
    return accessor.getLongs(rowId, count);
  }

  //
  // APIs dealing with floats
  //

  @Override
  public void putFloat(int rowId, float value) {
    writer.setFloat(rowId, value);
  }

  @Override
  public void putFloats(int rowId, int count, float value) {
    writer.setFloats(rowId, count, value);
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    writer.setFloats(rowId, count, src, srcIndex);
  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {
    writer.setFloats(rowId, count, src, srcIndex);
  }

  @Override
  public void putFloatsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {}

  @Override
  public float getFloat(int rowId) {
    return accessor.getFloat(rowId);
  }

  @Override
  public float[] getFloats(int rowId, int count) {
    return accessor.getFloats(rowId, count);
  }

  //
  // APIs dealing with doubles
  //

  @Override
  public void putDouble(int rowId, double value) {
    writer.setDouble(rowId, value);
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    writer.setDoubles(rowId, count, value);
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    writer.setDoubles(rowId, count, src, srcIndex);
  }

  @Override
  public void putDoublesLittleEndian(int rowId, int count, byte[] src, int srcIndex) {}

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {
    writer.setDoubles(rowId, count, src, srcIndex);
  }

  @Override
  public double getDouble(int rowId) {
    return accessor.getDouble(rowId);
  }

  @Override
  public double[] getDoubles(int rowId, int count) {
    return accessor.getDoubles(rowId, count);
  }

  //
  // APIs dealing with Arrays
  //

  @Override
  public int getArrayLength(int rowId) {
    return accessor.getArrayLength(rowId);
  }
  @Override
  public int getArrayOffset(int rowId) {
    return accessor.getArrayOffset(rowId);
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    writer.setArray(rowId, offset, length);
  }

  //
  // APIs dealing with Byte Arrays
  //

  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int length) {
    writer.setBytes(rowId, length, value, offset);
    return length;
  }

  //
  // APIs copied from original ArrowWritableColumnVector
  //

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId))
      return null;
    return accessor.getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isNullAt(rowId))
      return null;
    if (dataType() instanceof ArrayType) {
      UTF8String ret_0 = accessor.getUTF8String(rowId);
      for (int i = 0; i < ((ArrayAccessor) accessor).getArrayLength(rowId); i++) {
        ret_0 = UTF8String.concat(ret_0, getArray(rowId).getUTF8String(i));
      }
      return ret_0;
    } else {
      return accessor.getUTF8String(rowId);
    }
  }

  @Override
  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId))
      return null;
    return accessor.getBinary(rowId);
  }

  private abstract static class ArrowVectorAccessor {
    private final ValueVector vector;

    ArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
    }

    // TODO: should be final after removing ArrayAccessor workaround
    boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    final int getNullCount() {
      return vector.getNullCount();
    }

    final void close() {
      vector.close();
    }

    boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException();
    }

    boolean[] getBooleans(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    byte getByte(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte[] getBytes(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    short getShort(int rowId) {
      throw new UnsupportedOperationException();
    }

    short[] getShorts(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    int getInt(int rowId) {
      throw new UnsupportedOperationException();
    }

    int[] getInts(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    long getLong(int rowId) {
      throw new UnsupportedOperationException();
    }

    long[] getLongs(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    float getFloat(int rowId) {
      throw new UnsupportedOperationException();
    }

    float[] getFloats(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    double getDouble(int rowId) {
      throw new UnsupportedOperationException();
    }

    double[] getDoubles(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    Decimal getDecimal(int rowId, int precision, int scale) {
      throw new UnsupportedOperationException();
    }

    UTF8String getUTF8String(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException();
    }

    ColumnarArray getArray(int rowId) {
      throw new UnsupportedOperationException();
    }

    int getArrayLength(int rowId) {
      throw new UnsupportedOperationException();
    }

    int getArrayOffset(int rowId) {
      throw new UnsupportedOperationException();
    }
  }

  private static class BooleanAccessor extends ArrowVectorAccessor {
    private final BitVector accessor;

    BooleanAccessor(BitVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final boolean getBoolean(int rowId) {
      return accessor.get(rowId) == 1;
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(Boolean.toString(getBoolean(rowId)));
    }
  }

  private static class ByteAccessor extends ArrowVectorAccessor {
    private final TinyIntVector accessor;

    ByteAccessor(TinyIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte getByte(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(Byte.toString(accessor.get(rowId)));
    }
  }

  private static class ShortAccessor extends ArrowVectorAccessor {
    private final SmallIntVector accessor;

    ShortAccessor(SmallIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final short getShort(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(Short.toString(accessor.get(rowId)));
    }
  }

  private static class IntAccessor extends ArrowVectorAccessor {
    private final IntVector accessor;

    IntAccessor(IntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(Integer.toString(accessor.get(rowId)));
    }
  }

  private static class LongAccessor extends ArrowVectorAccessor {
    private final BigIntVector accessor;

    LongAccessor(BigIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(Long.toString(accessor.get(rowId)));
    }
  }

  private static class FloatAccessor extends ArrowVectorAccessor {
    private final Float4Vector accessor;

    FloatAccessor(Float4Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final float getFloat(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(Float.toString(accessor.get(rowId)));
    }
  }

  private static class DoubleAccessor extends ArrowVectorAccessor {
    private final Float8Vector accessor;

    DoubleAccessor(Float8Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final double getDouble(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(Double.toString(accessor.get(rowId)));
    }
  }

  private static class DecimalAccessor extends ArrowVectorAccessor {
    private final DecimalVector accessor;
    int precision = 0;
    int scale = 0;

    DecimalAccessor(DecimalVector vector) {
      super(vector);
      this.accessor = vector;
      this.precision = vector.getPrecision();
      this.scale = vector.getScale();
    }

    @Override
    final Decimal getDecimal(int rowId, int _precision, int _scale) {
      if (isNullAt(rowId))
        return null;
      return Decimal.apply(accessor.getObject(rowId), _precision, _scale);
    }

    final Decimal getDecimal(int rowId) {
      if (isNullAt(rowId))
        return null;
      return Decimal.apply(accessor.getObject(rowId), this.precision, this.scale);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(getDecimal(rowId).toString());
    }
  }

  private static class StringAccessor extends ArrowVectorAccessor {
    private final VarCharVector accessor;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    StringAccessor(VarCharVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      accessor.get(rowId, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      } else {
        return UTF8String.fromAddress(null,
            stringResult.buffer.memoryAddress() + stringResult.start,
            stringResult.end - stringResult.start);
      }
    }
  }

  private static class DictionaryEncodedStringAccessor extends ArrowVectorAccessor {
    private final IntVector index;
    private final VarCharVector dictionary;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    DictionaryEncodedStringAccessor(IntVector index, VarCharVector dictionary) {
      super(index);
      this.index = index;
      this.dictionary = dictionary;
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      int idx = index.get(rowId);
      dictionary.get(idx, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      } else {
        return UTF8String.fromAddress(null,
            stringResult.buffer.memoryAddress() + stringResult.start,
            stringResult.end - stringResult.start);
      }
    }
  }

  private static class BinaryAccessor extends ArrowVectorAccessor {
    private final VarBinaryVector accessor;

    BinaryAccessor(VarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }
  }

  private static class DictionaryEncodedBinaryAccessor extends ArrowVectorAccessor {
    private final IntVector index;
    private final VarBinaryVector dictionary;

    DictionaryEncodedBinaryAccessor(IntVector index, VarBinaryVector dictionary) {
      super(index);
      this.index = index;
      this.dictionary = dictionary;
    }

    @Override
    final byte[] getBinary(int rowId) {
      int idx = index.get(rowId);
      return dictionary.getObject(idx);
    }
  }

  private static class DateAccessor extends ArrowVectorAccessor {
    private final DateDayVector accessor;

    DateAccessor(DateDayVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      Date jDate = DateTimeUtils.toJavaDate((accessor.get(rowId)));
      return UTF8String.fromString(jDate.toString());
    }
  }

  private static class TimestampMicroAccessor extends ArrowVectorAccessor {
    private final TimeStampVector accessor;

    TimestampMicroAccessor(TimeStampVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class ArrayAccessor extends ArrowVectorAccessor {
    private final ListVector accessor;

    ArrayAccessor(ListVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    boolean isNullAt(int rowId) {
      // TODO: Workaround if vector has all non-null values, see ARROW-1948
      if (accessor.getValueCount() > 0 && accessor.getValidityBuffer().capacity() == 0) {
        return false;
      } else {
        return super.isNullAt(rowId);
      }
    }

    @Override
    public int getArrayLength(int rowId) {
      int index = rowId * ListVector.OFFSET_WIDTH;
      int start = accessor.getOffsetBuffer().getInt(index);
      int end = accessor.getOffsetBuffer().getInt(index + ListVector.OFFSET_WIDTH);
      return end - start;
    }

    @Override
    public int getArrayOffset(int rowId) {
      int index = rowId * ListVector.OFFSET_WIDTH;
      return accessor.getOffsetBuffer().getInt(index);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      return UTF8String.fromString(
          "Array[" + getArrayOffset(rowId) + "-" + getArrayLength(rowId) + "]");
    }
  }

  /**
   * Any call to "get" method will throw UnsupportedOperationException.
   *
   * Access struct values in a ArrowWritableColumnVector doesn't use this accessor.
   * Instead, it uses getStruct() method defined in the parent class. Any call to "get"
   * method in this class is a bug in the code.
   *
   */
  private static class StructAccessor extends ArrowVectorAccessor {
    StructAccessor(StructVector vector) {
      super(vector);
    }
  }

  /* Arrow Vector Writer */
  private abstract static class ArrowVectorWriter {
    private final ValueVector vector;

    ArrowVectorWriter(ValueVector vector) {
      this.vector = vector;
    }

    final void close() {
      vector.close();
    }

    void setNull(int rowId) {
      throw new UnsupportedOperationException();
    }

    void setNotNull(int rowId) {
      throw new UnsupportedOperationException();
    }

    void setNulls(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    void setNotNulls(int rowId, int count) {
      throw new UnsupportedOperationException();
    }

    void setBoolean(int rowId, boolean value) {
      throw new UnsupportedOperationException();
    }

    void setBooleans(int rowId, int count, boolean value) {
      throw new UnsupportedOperationException();
    }

    void setByte(int rowId, byte value) {
      throw new UnsupportedOperationException();
    }

    void setBytes(int rowId, int count, byte value) {
      throw new UnsupportedOperationException();
    }

    void setBytes(int rowId, int count, byte[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setShort(int rowId, short value) {
      throw new UnsupportedOperationException();
    }

    void setShorts(int rowId, int count, short value) {
      throw new UnsupportedOperationException();
    }

    void setShorts(int rowId, int count, short[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setShorts(int rowId, int count, byte[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setInt(int rowId, int value) {
      throw new UnsupportedOperationException();
    }

    void setInts(int rowId, int count, int value) {
      throw new UnsupportedOperationException();
    }

    void setInts(int rowId, int count, int[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setInts(int rowId, int count, byte[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setLong(int rowId, long value) {
      throw new UnsupportedOperationException();
    }

    void setLongs(int rowId, int count, long value) {
      throw new UnsupportedOperationException();
    }

    void setLongs(int rowId, int count, long[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setLongs(int rowId, int count, byte[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setFloat(int rowId, float value) {
      throw new UnsupportedOperationException();
    }

    void setFloats(int rowId, int count, float value) {
      throw new UnsupportedOperationException();
    }

    void setFloats(int rowId, int count, float[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setFloats(int rowId, int count, byte[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setDouble(int rowId, double value) {
      throw new UnsupportedOperationException();
    }

    void setDoubles(int rowId, int count, double value) {
      throw new UnsupportedOperationException();
    }

    void setDoubles(int rowId, int count, double[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setDoubles(int rowId, int count, byte[] src, int srcIndex) {
      throw new UnsupportedOperationException();
    }

    void setArray(int rowId, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    void setByteArray(int rowId, byte[] value, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    void appendBytes(byte[] value, int offset, int length) {
      throw new UnsupportedOperationException();
    }

    void setBytes(int rowId, BigDecimal value) {
      throw new UnsupportedOperationException();
    }
  }

  private static class BooleanWriter extends ArrowVectorWriter {
    private final BitVector writer;

    BooleanWriter(BitVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setBoolean(int rowId, boolean value) {
      writer.setSafe(rowId, value ? 1 : 0);
    }

    @Override
    final void setBooleans(int rowId, int count, boolean value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value ? 1 : 0);
      }
    }
  }

  private static class ByteWriter extends ArrowVectorWriter {
    private final TinyIntVector writer;

    ByteWriter(TinyIntVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setByte(int rowId, byte value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setBytes(int rowId, int count, byte value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value);
      }
    }

    @Override
    final void setBytes(int rowId, int count, byte[] src, int srcIndex) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, src[srcIndex + i]);
      }
    }
  }

  private static class ShortWriter extends ArrowVectorWriter {
    private final SmallIntVector writer;

    ShortWriter(SmallIntVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setShort(int rowId, short value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setShorts(int rowId, int count, short value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value);
      }
    }

    @Override
    final void setShorts(int rowId, int count, short[] src, int srcIndex) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, src[srcIndex + i]);
      }
    }

    @Override
    final void setShorts(int rowId, int count, byte[] src, int srcIndex) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, src[srcIndex + i]);
      }
    }
  }

  private static class IntWriter extends ArrowVectorWriter {
    private final IntVector writer;

    IntWriter(IntVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setInt(int rowId, int value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setInts(int rowId, int count, int value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value);
      }
    }

    @Override
    final void setInts(int rowId, int count, int[] src, int srcIndex) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, src[srcIndex + i]);
      }
    }

    @Override
    final void setInts(int rowId, int count, byte[] src, int srcIndex) {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      for (int i = 0; i < count; i++, srcOffset += 4) {
        writer.setSafe(rowId + i, Platform.getInt(src, srcOffset));
      }
    }

    @Override
    void setIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      for (int i = 0; i < count; i++, srcOffset += 4) {
        int tmp = Platform.getInt(src, srcOffset);
        if (bigEndianPlatform) {
          tmp = java.lang.Integer.reverseBytes(tmp);
        }
        writer.setSafe(rowId + i, tmp);
      }
    }
  }

  private static class LongWriter extends ArrowVectorWriter {
    private final BigIntVector writer;

    LongWriter(BigIntVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setLong(int rowId, long value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setLongs(int rowId, int count, long value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value);
      }
    }

    @Override
    final void setLongs(int rowId, int count, long[] src, int srcIndex) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, src[srcIndex + i]);
      }
    }

    @Override
    final void setLongs(int rowId, int count, byte[] src, int srcIndex) {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      for (int i = 0; i < count; i++, srcOffset += 8) {
        writer.setSafe(rowId + i, Platform.getLong(src, srcOffset));
      }
    }

    @Override
    final void setDouble(int rowId, double value) {
      long val = (long) value;
      writer.setSafe(rowId, val);
    }

    @Override
    void setLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {
      int srcOffset = srcIndex + Platform.BYTE_ARRAY_OFFSET;
      for (int i = 0; i < count; i++, srcOffset += 8) {
        long tmp = Platform.getLong(src, srcOffset);
        if (bigEndianPlatform) {
          tmp = java.lang.Long.reverseBytes(tmp);
        }
        writer.setSafe(rowId + i, tmp);
      }
    }
  }

  private static class FloatWriter extends ArrowVectorWriter {
    private final Float4Vector writer;

    FloatWriter(Float4Vector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setFloat(int rowId, float value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setFloats(int rowId, int count, float value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value);
      }
    }

    @Override
    final void setFloats(int rowId, int count, float[] src, int srcIndex) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, src[srcIndex + i]);
      }
    }
  }

  private static class DoubleWriter extends ArrowVectorWriter {
    private final Float8Vector writer;

    DoubleWriter(Float8Vector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setDouble(int rowId, double value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setDoubles(int rowId, int count, double value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value);
      }
    }

    @Override
    final void setDoubles(int rowId, int count, double[] src, int srcIndex) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, src[srcIndex + i]);
      }
    }
  }

  private static class DecimalWriter extends ArrowVectorWriter {
    private final DecimalVector writer;

    DecimalWriter(DecimalVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setInt(int rowId, int value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setLong(int rowId, long value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setBytes(int rowId, BigDecimal value) {
      writer.setSafe(rowId, value);
    }
  }

  private static class StringWriter extends ArrowVectorWriter {
    private final VarCharVector writer;
    private int rowId;

    StringWriter(VarCharVector vector) {
      super(vector);
      this.writer = vector;
      this.rowId = 0;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setBytes(int rowId, int count, byte[] src, int srcIndex) {
      writer.setSafe(rowId, src, srcIndex, count);
    }

    @Override
    final void appendBytes(byte[] value, int offset, int length) {
      writer.setSafe(rowId, value, offset, length);
      rowId++;
    }
  }

  private static class BinaryWriter extends ArrowVectorWriter {
    private final VarBinaryVector writer;

    BinaryWriter(VarBinaryVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }

    @Override
    final void setBytes(int rowId, int count, byte[] src, int srcIndex) {
      writer.setSafe(rowId, src, srcIndex, count);
    }
  }

  private static class DateWriter extends ArrowVectorWriter {
    private final DateDayVector writer;

    DateWriter(DateDayVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    final void setInt(int rowId, int value) {
      writer.set(rowId, value);
    }

    @Override
    void setInts(int rowId, int count, int value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value);
      }
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }
  }

  private static class TimestampMicroWriter extends ArrowVectorWriter {
    private final TimeStampVector writer;

    TimestampMicroWriter(TimeStampVector vector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    void setLongs(int rowId, int count, long value) {
      for (int i = 0; i < count; i++) {
        writer.setSafe(rowId + i, value);
      }
    }

    @Override
    void setLong(int rowId, long value) {
      writer.setSafe(rowId, value);
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }

    @Override
    final void setNulls(int rowId, int count) {
      for (int i = 0; i < count; i++) {
        writer.setNull(rowId + i);
      }
    }
  }

  private static class ArrayWriter extends ArrowVectorWriter {
    private final ListVector writer;

    ArrayWriter(ListVector vector, ArrowVectorWriter elementVector) {
      super(vector);
      this.writer = vector;
    }

    @Override
    void setArray(int rowId, int offset, int length) {
      int index = rowId * ListVector.OFFSET_WIDTH;
      writer.getOffsetBuffer().setInt(index, offset);
      writer.getOffsetBuffer().setInt(index + ListVector.OFFSET_WIDTH, offset + length);
      writer.setNotNull(rowId);
    }

    @Override
    final void setNull(int rowId) {
      writer.setNull(rowId);
    }
  }

  private static class StructWriter extends ArrowVectorWriter {
    StructWriter(StructVector vector, ArrowVectorWriter[] children) {
      super(vector);
    }
  }
}
