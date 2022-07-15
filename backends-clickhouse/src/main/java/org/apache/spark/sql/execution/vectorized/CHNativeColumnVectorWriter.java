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

package org.apache.spark.sql.execution.vectorized;

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;

public class CHNativeColumnVectorWriter implements NativeColumnVectorWriter {

  public CHNativeColumnVectorWriter() {

  }

  @Override
  public void putNotNull(int rowId) {

  }

  @Override
  public void putNull(int rowId) {

  }

  @Override
  public void putNulls(int rowId, int count) {

  }

  @Override
  public void putNotNulls(int rowId, int count) {

  }

  @Override
  public void putBoolean(int rowId, boolean value) {

  }

  @Override
  public void putBooleans(int rowId, int count, boolean value) {

  }

  @Override
  public void putByte(int rowId, byte value) {

  }

  @Override
  public void putBytes(int rowId, int count, byte value) {

  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {

  }

  @Override
  public void putShort(int rowId, short value) {

  }

  @Override
  public void putShorts(int rowId, int count, short value) {

  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {

  }

  @Override
  public void putShorts(int rowId, int count, byte[] src, int srcIndex) {

  }

  @Override
  public void putInt(int rowId, int value) {

  }

  @Override
  public void putInts(int rowId, int count, int value) {

  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {

  }

  @Override
  public void putInts(int rowId, int count, byte[] src, int srcIndex) {

  }

  @Override
  public void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {

  }

  @Override
  public void putLong(int rowId, long value) {

  }

  @Override
  public void putLongs(int rowId, int count, long value) {

  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {

  }

  @Override
  public void putLongs(int rowId, int count, byte[] src, int srcIndex) {

  }

  @Override
  public void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {

  }

  @Override
  public void putFloat(int rowId, float value) {

  }

  @Override
  public void putFloats(int rowId, int count, float value) {

  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {

  }

  @Override
  public void putFloats(int rowId, int count, byte[] src, int srcIndex) {

  }

  @Override
  public void putFloatsLittleEndian(int rowId, int count, byte[] src, int srcIndex) {

  }

  @Override
  public void putDouble(int rowId, double value) {

  }

  @Override
  public void putDoubles(int rowId, int count, double value) {

  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {

  }

  @Override
  public void putDoubles(int rowId, int count, byte[] src, int srcIndex) {

  }

  @Override
  public void putDoublesLittleEndian(int rowId, int count, byte[] src, int srcIndex) {

  }

  @Override
  public void putArray(int rowId, int offset, int length) {

  }

  @Override
  public int putByteArray(int rowId, byte[] value, int offset, int count) {
    return 0;
  }

  @Override
  public int putByteArray(int rowId, byte[] value) {
    return 0;
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return null;
  }

  @Override
  public void putDecimal(int rowId, Decimal value, int precision) {

  }

  @Override
  public void putInterval(int rowId, CalendarInterval value) {

  }

  @Override
  public void close() {

  }

  @Override
  public long getNativeAddress() {
    return 0;
  }
}
