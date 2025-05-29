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

import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/*
 * This module is used to convert column vector to flink generic rows.
 * BinaryRowData is not supported here.
 */
public abstract class ArrowVectorAccessor {
  private interface AccessorBuilder {
    ArrowVectorAccessor build(FieldVector vector);
  };

  // Exact class matches
  private static final Map<Class<? extends FieldVector>, AccessorBuilder> accessorBuilders =
      Map.ofEntries(
          Map.entry(BitVector.class, vector -> new BooleanVectorAccessor(vector)),
          Map.entry(IntVector.class, vector -> new IntVectorAccessor(vector)),
          Map.entry(BigIntVector.class, vector -> new BigIntVectorAccessor(vector)),
          Map.entry(Float8Vector.class, vector -> new DoubleVectorAccessor(vector)),
          Map.entry(TimeStampMicroVector.class, vector -> new TimeStampMicroVectorAccessor(vector)),
          Map.entry(VarCharVector.class, vector -> new VarCharVectorAccessor(vector)),
          Map.entry(StructVector.class, vector -> new StructVectorAccessor(vector)),
          Map.entry(ListVector.class, vector -> new ListVectorAccessor(vector)),
          Map.entry(MapVector.class, vector -> new MapVectorAccessor(vector)));

  public static ArrowVectorAccessor create(FieldVector vector) {
    if (vector == null) {
      throw new IllegalArgumentException(
          "ArrowVectorAccessor. Cannot create accessor for null vector.");
    }
    AccessorBuilder builder = accessorBuilders.get(vector.getClass());
    if (builder == null) {
      throw new UnsupportedOperationException(
          "ArrowVectorAccessor. Unsupported vector type: " + vector.getClass().getName());
    }
    return builder.build(vector);
  }

  // A general method to extract values from the vector.
  public Object get(int rowIndex) {
    throw new UnsupportedOperationException("get not supported");
  }
}

class BooleanVectorAccessor extends ArrowVectorAccessor {
  private final BitVector vector;

  public BooleanVectorAccessor(FieldVector vector) {
    this.vector = (BitVector) vector;
  }

  @Override
  public Object get(int rowIndex) {
    return vector.get(rowIndex) != 0;
  }
}

class IntVectorAccessor extends ArrowVectorAccessor {
  private final IntVector vector;

  public IntVectorAccessor(FieldVector vector) {
    this.vector = (IntVector) vector;
  }

  @Override
  public Object get(int rowIndex) {
    return vector.get(rowIndex);
  }
}

class BigIntVectorAccessor extends ArrowVectorAccessor {
  private final BigIntVector vector;

  public BigIntVectorAccessor(FieldVector vector) {
    this.vector = (BigIntVector) vector;
  }

  @Override
  public Object get(int rowIndex) {
    return vector.get(rowIndex);
  }
}

class DoubleVectorAccessor extends ArrowVectorAccessor {
  private final Float8Vector vector;

  public DoubleVectorAccessor(FieldVector vector) {
    this.vector = (Float8Vector) vector;
  }

  @Override
  public Object get(int rowIndex) {
    return vector.get(rowIndex);
  }
}

class TimeStampMicroVectorAccessor extends ArrowVectorAccessor {
  private final TimeStampMicroVector vector;

  public TimeStampMicroVectorAccessor(FieldVector vector) {
    this.vector = (TimeStampMicroVector) vector;
  }

  @Override
  public Object get(int rowIndex) {
    long microSeconds = vector.get(rowIndex);
    long millis = microSeconds / 1000;
    int nanosOfMillisecond = (int) (microSeconds % 1000) * 1000;
    return TimestampData.fromEpochMillis(millis, nanosOfMillisecond);
  }
}

class VarCharVectorAccessor extends ArrowVectorAccessor {
  private final VarCharVector vector;

  public VarCharVectorAccessor(FieldVector vector) {
    this.vector = (VarCharVector) vector;
  }

  @Override
  public Object get(int rowIndex) {
    return BinaryStringData.fromBytes(vector.get(rowIndex));
  }
}

class StructVectorAccessor extends ArrowVectorAccessor {
  private final StructVector vector;
  private final List<ArrowVectorAccessor> fieldAccessors;
  private int fieldCount = 0;

  public StructVectorAccessor(FieldVector vector) {
    this.vector = (StructVector) vector;
    // size() returns the number of child vectors
    this.fieldCount = this.vector.size();
    this.fieldAccessors = new ArrayList<>(fieldCount);
    for (int i = 0; i < this.fieldCount; i++) {
      FieldVector fieldVector = (FieldVector) this.vector.getChildByOrdinal(i);
      this.fieldAccessors.add(i, ArrowVectorAccessor.create(fieldVector));
    }
  }

  @Override
  public Object get(int rowIndex) {
    int fieldCount = vector.size();
    Object[] fieldValues = new Object[fieldCount];
    for (int i = 0; i < fieldCount; i++) {
      fieldValues[i] = fieldAccessors.get(i).get(rowIndex);
    }
    return GenericRowData.of(fieldValues);
  }
}

class ListVectorAccessor extends ArrowVectorAccessor {
  private ListVector vector;
  private ArrowVectorAccessor elementAccessor;

  public ListVectorAccessor(FieldVector vector) {
    this.vector = (ListVector) vector;
    FieldVector elementVector = this.vector.getDataVector();
    this.elementAccessor = ArrowVectorAccessor.create(elementVector);
  }

  @Override
  public Object get(int rowIndex) {
    int startIndex = vector.getElementStartIndex(rowIndex);
    int endIndex = vector.getElementEndIndex(rowIndex);
    Object[] elements = new Object[endIndex - startIndex];
    for (int i = startIndex; i < endIndex; i++) {
      elements[i - startIndex] = elementAccessor.get(i);
    }
    return new GenericArrayData(elements);
  }
}

// In Arrow, the internal implementation of a map vector is an array vector.
class MapVectorAccessor extends ArrowVectorAccessor {
  private final MapVector vector;
  private StructVector entriesVector;
  private ArrowVectorAccessor keyAccessor;
  private ArrowVectorAccessor valueAccessor;

  public MapVectorAccessor(FieldVector vector) {
    this.vector = (MapVector) vector;
    this.entriesVector = (StructVector) this.vector.getDataVector();
    FieldVector keyVector = this.entriesVector.getChild(MapVector.KEY_NAME);
    FieldVector valueVector = this.entriesVector.getChild(MapVector.VALUE_NAME);
    this.keyAccessor = ArrowVectorAccessor.create(keyVector);
    this.valueAccessor = ArrowVectorAccessor.create(valueVector);
  }

  @Override
  public Object get(int rowIndex) {
    int startIndex = vector.getElementStartIndex(rowIndex);
    int endIndex = vector.getElementEndIndex(rowIndex);
    Map<Object, Object> mapEntries = new LinkedHashMap<>();
    for (int i = startIndex; i < endIndex; i++) {
      Object key = keyAccessor.get(i);
      Object value = valueAccessor.get(i);
      mapEntries.put(key, value);
    }
    return new GenericMapData(mapEntries);
  }
}
