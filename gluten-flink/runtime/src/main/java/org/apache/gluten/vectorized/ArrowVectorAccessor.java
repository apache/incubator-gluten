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

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.binary.BinaryStringData;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;

import java.util.ArrayList;
import java.util.List;

/*
 * This module is used to convert column vector to flink generic rows.
 * BinaryRowData is not supported here.
 */
public abstract class ArrowVectorAccessor {
  public static ArrowVectorAccessor create(FieldVector vector) {
    if (vector instanceof BitVector) {
      return new BooleanVectorAccessor(vector);
    } else if (vector instanceof IntVector) {
      return new IntVectorAccessor(vector);
    } else if (vector instanceof BigIntVector) {
      return new BigIntVectorAccessor(vector);
    } else if (vector instanceof Float8Vector) {
      return new DoubleVectorAccessor(vector);
    } else if (vector instanceof VarCharVector) {
      return new VarCharVectorAccessor(vector);
    } else if (vector instanceof StructVector) {
      return new StructVectorAccessor(vector);
    } else {
      throw new UnsupportedOperationException(
          "ArrowVectorAccessor. Unsupported type: " + vector.getClass().getName());
    }
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
