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

public interface NativeColumnVectorWriter {

  /**
   * Sets null/not null to the value at rowId.
   */
  void putNotNull(int rowId);
  void putNull(int rowId);

  /**
   * Sets null/not null to the values at [rowId, rowId + count).
   */
  void putNulls(int rowId, int count);
  void putNotNulls(int rowId, int count);

  /**
   * Sets `value` to the value at rowId.
   */
  void putBoolean(int rowId, boolean value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  void putBooleans(int rowId, int count, boolean value);

  /**
   * Sets `value` to the value at rowId.
   */
  void putByte(int rowId, byte value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  void putBytes(int rowId, int count, byte value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  void putBytes(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets `value` to the value at rowId.
   */
  void putShort(int rowId, short value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  void putShorts(int rowId, int count, short value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  void putShorts(int rowId, int count, short[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 2]) to [rowId, rowId + count)
   * The data in src must be 2-byte platform native endian shorts.
   */
  void putShorts(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets `value` to the value at rowId.
   */
  void putInt(int rowId, int value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  void putInts(int rowId, int count, int value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  void putInts(int rowId, int count, int[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count)
   * The data in src must be 4-byte platform native endian ints.
   */
  void putInts(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count)
   * The data in src must be 4-byte little endian ints.
   */
  void putIntsLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets `value` to the value at rowId.
   */
  void putLong(int rowId, long value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  void putLongs(int rowId, int count, long value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  void putLongs(int rowId, int count, long[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 8]) to [rowId, rowId + count)
   * The data in src must be 8-byte platform native endian longs.
   */
  void putLongs(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets values from [src + srcIndex, src + srcIndex + count * 8) to [rowId, rowId + count)
   * The data in src must be 8-byte little endian longs.
   */
  void putLongsLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets `value` to the value at rowId.
   */
  void putFloat(int rowId, float value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  void putFloats(int rowId, int count, float value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  void putFloats(int rowId, int count, float[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count)
   * The data in src must be ieee formatted floats in platform native endian.
   */
  void putFloats(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 4]) to [rowId, rowId + count)
   * The data in src must be ieee formatted floats in little endian.
   */
  void putFloatsLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets `value` to the value at rowId.
   */
  void putDouble(int rowId, double value);

  /**
   * Sets value to [rowId, rowId + count).
   */
  void putDoubles(int rowId, int count, double value);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count]) to [rowId, rowId + count)
   */
  void putDoubles(int rowId, int count, double[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 8]) to [rowId, rowId + count)
   * The data in src must be ieee formatted doubles in platform native endian.
   */
  void putDoubles(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Sets values from [src[srcIndex], src[srcIndex + count * 8]) to [rowId, rowId + count)
   * The data in src must be ieee formatted doubles in little endian.
   */
  void putDoublesLittleEndian(int rowId, int count, byte[] src, int srcIndex);

  /**
   * Puts a byte array that already exists in this column.
   */
  void putArray(int rowId, int offset, int length);

  /**
   * Sets values from [value + offset, value + offset + count) to the values at rowId.
   */
  int putByteArray(int rowId, byte[] value, int offset, int count);

  int putByteArray(int rowId, byte[] value);

  Decimal getDecimal(int rowId, int precision, int scale);

  void putDecimal(int rowId, Decimal value, int precision);

  void putInterval(int rowId, CalendarInterval value);

  void close();

  long getNativeAddress();
}
