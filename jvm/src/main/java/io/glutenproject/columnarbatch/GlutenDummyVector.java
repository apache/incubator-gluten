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

package io.glutenproject.columnarbatch;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

public class GlutenDummyVector extends ColumnVector {

  protected GlutenDummyVector(DataType type) {
    super(DataTypes.NullType);
  }

  @Override
  public void close() {

  }

  @Override
  public boolean hasNull() {
    return false;
  }

  @Override
  public int numNulls() {
    return 0;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return false;
  }

  @Override
  public boolean getBoolean(int rowId) {
    return false;
  }

  @Override
  public byte getByte(int rowId) {
    return 0;
  }

  @Override
  public short getShort(int rowId) {
    return 0;
  }

  @Override
  public int getInt(int rowId) {
    return 0;
  }

  @Override
  public long getLong(int rowId) {
    return 0;
  }

  @Override
  public float getFloat(int rowId) {
    return 0;
  }

  @Override
  public double getDouble(int rowId) {
    return 0;
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    return null;
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    return null;
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return null;
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return null;
  }

  @Override
  public byte[] getBinary(int rowId) {
    return new byte[0];
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    return null;
  }
}
