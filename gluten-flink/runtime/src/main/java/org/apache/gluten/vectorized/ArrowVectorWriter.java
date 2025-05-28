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
package org.apache.gluten.vectorized;

import io.github.zhztheplayer.velox4j.type.*;

import org.apache.flink.table.data.RowData;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.*;

import java.util.ArrayList;
import java.util.List;

public abstract class ArrowVectorWriter {
  public static ArrowVectorWriter create(
      String fieldName, Type fieldType, BufferAllocator allocator) {
    return create(fieldName, fieldType, allocator, null);
  }

  protected static ArrowVectorWriter create(
      String fieldName, Type fieldType, BufferAllocator allocator, FieldVector vector) {
    if (vector == null) {
      // Build an empty vector
      vector = FieldVectorCreator.create(fieldName, fieldType, false, allocator, null);
    }
    if (fieldType instanceof IntegerType) {
      return new IntVectorWriter(fieldType, allocator, vector);
    } else if (fieldType instanceof BooleanType) {
      return new BooleanVectorWriter(fieldType, allocator, vector);
    } else if (fieldType instanceof BigIntType) {
      return new BigIntVectorWriter(fieldType, allocator, vector);
    } else if (fieldType instanceof DoubleType) {
      return new Float8VectorWriter(fieldType, allocator, vector);
    } else if (fieldType instanceof VarCharType) {
      return new VarCharVectorWriter(fieldType, allocator, vector);
    } else if (fieldType instanceof TimestampType) {
      return new TimestampVectorWriter(fieldType, allocator, vector);
    } else if (fieldType instanceof RowType) {
      return new StructVectorWriter(fieldType, allocator, vector);
    } else {
      throw new UnsupportedOperationException("ArrowVectorWriter. Unsupported type: " + fieldType);
    }
  }

  public void write(int fieldIndex, RowData rowData) {
    throw new UnsupportedOperationException("assign is not supported");
  }

  public void write(int fieldIndex, List<RowData> rowData) {
    for (RowData row : rowData) {
      write(fieldIndex, row);
    }
  }

  protected FieldVector vector = null;
  protected int valueCount = 0;

  ArrowVectorWriter(FieldVector vector) {
    this.vector = vector;
  }

  FieldVector getVector() {
    return vector;
  }

  void finish() {
    vector.setValueCount(valueCount);
  }
}
// Build FieldVector from Type.
class FieldVectorCreator {
  public static FieldVector create(
      String name, Type dataType, boolean nullable, BufferAllocator allocator, String timeZoneId) {
    Field field = toArrowField(name, dataType, nullable, timeZoneId);
    return field.createVector(allocator);
  }

  private static ArrowType toArrowType(Type dataType, String timeZoneId) {
    if (dataType instanceof BooleanType) {
      return ArrowType.Bool.INSTANCE;
    } else if (dataType instanceof IntegerType) {
      return new ArrowType.Int(8 * 4, true);
    } else if (dataType instanceof BigIntType) {
      return new ArrowType.Int(8 * 8, true);
    } else if (dataType instanceof DoubleType) {
      return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    } else if (dataType instanceof VarCharType) {
      return ArrowType.Utf8.INSTANCE;
    } else if (dataType instanceof TimestampType) {
      if (timeZoneId == null) {
        return new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC");
      } else {
        return new ArrowType.Timestamp(TimeUnit.MILLISECOND, timeZoneId);
      }
    } else {
      throw new UnsupportedOperationException("Unsupported type: " + dataType);
    }
  }

  private static Field toArrowField(
      String name, Type dataType, boolean nullable, String timeZoneId) {
    if (dataType instanceof ArrayType) {
      throw new UnsupportedOperationException("ArrayType is not supported");
    } else if (dataType instanceof MapType) {
      throw new UnsupportedOperationException("MapType is not supported");
    } else if (dataType instanceof RowType) {
      RowType structType = (RowType) dataType;
      List<String> fieldNames = structType.getNames();
      List<Type> fieldTypes = structType.getChildren();
      List<Field> subFields = new ArrayList<>();
      for (int i = 0; i < structType.getChildren().size(); ++i) {
        subFields.add(toArrowField(fieldNames.get(i), fieldTypes.get(i), nullable, timeZoneId));
      }
      FieldType strcutType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null);
      return new Field(name, strcutType, subFields);
    } else {
      // TODO: support nullable
      ArrowType arrowType = toArrowType(dataType, timeZoneId);
      FieldType fieldType = new FieldType(nullable, arrowType, null);
      return new Field(name, fieldType, new ArrayList<>());
    }
  }
}

class IntVectorWriter extends ArrowVectorWriter {
  private final IntVector intVector;

  public IntVectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);
    this.intVector = (IntVector) vector;
  }

  @Override
  public void write(int fieldIndex, RowData rowData) {
    intVector.setSafe(valueCount, rowData.getInt(fieldIndex));
    valueCount++;
  }
}

class BooleanVectorWriter extends ArrowVectorWriter {
  private final BitVector bitVector;

  public BooleanVectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);
    this.bitVector = (BitVector) vector;
  }

  @Override
  public void write(int fieldIndex, RowData rowData) {
    bitVector.setSafe(valueCount, rowData.getBoolean(fieldIndex) ? 1 : 0);
    valueCount++;
  }
}

class BigIntVectorWriter extends ArrowVectorWriter {
  private final BigIntVector bigIntvector;

  public BigIntVectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);
    this.bigIntvector = (BigIntVector) vector;
  }

  @Override
  public void write(int fieldIndex, RowData rowData) {
    bigIntvector.setSafe(valueCount, rowData.getLong(fieldIndex));
    valueCount++;
  }
}

class Float8VectorWriter extends ArrowVectorWriter {
  private final Float8Vector float8Vector;

  public Float8VectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);
    this.float8Vector = (Float8Vector) vector;
  }

  @Override
  public void write(int fieldIndex, RowData rowData) {
    float8Vector.setSafe(valueCount, rowData.getDouble(fieldIndex));
    valueCount++;
  }
}

class VarCharVectorWriter extends ArrowVectorWriter {
  private final VarCharVector varCharVector;

  public VarCharVectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);
    this.varCharVector = (VarCharVector) vector;
  }

  @Override
  public void write(int fieldIndex, RowData rowData) {
    varCharVector.setSafe(valueCount, rowData.getString(fieldIndex).toBytes());
    valueCount++;
  }
}

class TimestampVectorWriter extends ArrowVectorWriter {
  private final TimeStampMilliVector tsVector;

  public TimestampVectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);
    this.tsVector = (TimeStampMilliVector) vector;
  }

  @Override
  public void write(int fieldIndex, RowData rowData) {
    // TODO: support precision
    tsVector.setSafe(valueCount, rowData.getTimestamp(fieldIndex, 3).getMillisecond());
    valueCount++;
  }
}

class StructVectorWriter extends ArrowVectorWriter {
  private int fieldCounts = 0;
  BufferAllocator allocator;
  private List<ArrowVectorWriter> subFieldWriters;
  private StructVector strctVector;

  public StructVectorWriter(Type fieldType, BufferAllocator allocator, FieldVector vector) {
    super(vector);
    this.strctVector = (StructVector) vector;
    RowType rowType = (RowType) fieldType;
    List<String> subFieldNames = rowType.getNames();
    subFieldWriters = new ArrayList<>();
    for (int i = 0; i < subFieldNames.size(); ++i) {
      subFieldWriters.add(
          ArrowVectorWriter.create(
              subFieldNames.get(i),
              rowType.getChildren().get(i),
              allocator,
              (FieldVector) (this.strctVector.getChildByOrdinal(i))));
    }
    fieldCounts = subFieldNames.size();
  }

  @Override
  public void write(int fieldIndex, RowData rowData) {
    // TODO: support nullable
    RowData subRowData = rowData.getRow(fieldIndex, fieldCounts);
    strctVector.setIndexDefined(valueCount);
    for (int i = 0; i < fieldCounts; i++) {
      subFieldWriters.get(i).write(i, subRowData);
    }
    valueCount++;
  }

  @Override
  public void finish() {
    strctVector.setValueCount(valueCount);
    for (int i = 0; i < fieldCounts; i++) {
      subFieldWriters.get(i).finish();
    }
  }
}
