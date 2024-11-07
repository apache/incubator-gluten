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
package org.apache.spark.sql.vectorized;

import org.apache.gluten.exception.GlutenException;

import java.lang.reflect.Field;

public class SparkColumnarBatchUtil {

  private static final Field FIELD_COLUMNS;
  private static final Field FIELD_COLUMNAR_BATCH_ROW;

  static {
    try {
      Field f = ColumnarBatch.class.getDeclaredField("columns");
      f.setAccessible(true);
      FIELD_COLUMNS = f;
      Field row = ColumnarBatch.class.getDeclaredField("row");
      row.setAccessible(true);
      FIELD_COLUMNAR_BATCH_ROW = row;
    } catch (NoSuchFieldException e) {
      throw new GlutenException(e);
    }
  }

  private static void setColumnarBatchRow(
      ColumnarBatch from, ColumnVector[] columns, ColumnarBatch target) {
    ColumnarBatchRow newRow = new ColumnarBatchRow(columns);
    try {
      ColumnarBatchRow row = (ColumnarBatchRow) FIELD_COLUMNAR_BATCH_ROW.get(from);
      newRow.rowId = row.rowId;
      FIELD_COLUMNAR_BATCH_ROW.set(target, newRow);
    } catch (IllegalAccessException e) {
      throw new GlutenException(e);
    }
  }

  public static void transferVectors(ColumnarBatch from, ColumnarBatch target) {
    try {
      if (target.numCols() != from.numCols()) {
        throw new IllegalStateException();
      }
      final ColumnVector[] newVectors = new ColumnVector[from.numCols()];
      for (int i = 0; i < target.numCols(); i++) {
        newVectors[i] = from.column(i);
      }
      FIELD_COLUMNS.set(target, newVectors);
      setColumnarBatchRow(from, newVectors, target);
    } catch (IllegalAccessException e) {
      throw new GlutenException(e);
    }
  }
}
