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
package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

trait IFakeRowAdaptor

class FakeRow(val batch: ColumnarBatch) extends InternalRow {
  override def numFields: Int = throw new UnsupportedOperationException()

  override def setNullAt(i: Int): Unit = throw new UnsupportedOperationException()

  override def update(i: Int, value: Any): Unit = throw new UnsupportedOperationException()

  override def copy(): InternalRow = throw new UnsupportedOperationException()

  override def isNullAt(ordinal: Int): Boolean = throw new UnsupportedOperationException()

  override def getBoolean(ordinal: Int): Boolean = throw new UnsupportedOperationException()

  override def getByte(ordinal: Int): Byte = throw new UnsupportedOperationException()

  override def getShort(ordinal: Int): Short = throw new UnsupportedOperationException()

  override def getInt(ordinal: Int): Int = throw new UnsupportedOperationException()

  override def getLong(ordinal: Int): Long = throw new UnsupportedOperationException()

  override def getFloat(ordinal: Int): Float = throw new UnsupportedOperationException()

  override def getDouble(ordinal: Int): Double = throw new UnsupportedOperationException()

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
    throw new UnsupportedOperationException()

  override def getUTF8String(ordinal: Int): UTF8String =
    throw new UnsupportedOperationException()

  override def getBinary(ordinal: Int): Array[Byte] = throw new UnsupportedOperationException()

  override def getInterval(ordinal: Int): CalendarInterval =
    throw new UnsupportedOperationException()

  override def getStruct(ordinal: Int, numFields: Int): InternalRow =
    throw new UnsupportedOperationException()

  override def getArray(ordinal: Int): ArrayData = throw new UnsupportedOperationException()

  override def getMap(ordinal: Int): MapData = throw new UnsupportedOperationException()

  override def get(ordinal: Int, dataType: DataType): AnyRef =
    throw new UnsupportedOperationException()
}
