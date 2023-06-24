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

import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public final class GlutenColumnarBatches {

  private GlutenColumnarBatches() {}

  public static ColumnarBatch create(long nativeHandle) {
    final IndicatorVector iv = new IndicatorVector(nativeHandle);
    int numColumns = Math.toIntExact(iv.getNumColumns());
    int numRows = Math.toIntExact(iv.getNumRows());
    if (numColumns == 0) {
      return new ColumnarBatch(new ColumnVector[0], numRows);
    }
    final ColumnVector[] columnVectors = new ColumnVector[numColumns];
    columnVectors[0] = iv;
    long numPlaceholders = numColumns - 1;
    for (int i = 0; i < numPlaceholders; i++) {
      final PlaceholderVector pv = PlaceholderVector.INSTANCE;
      columnVectors[i + 1] = pv;
    }
    return new ColumnarBatch(columnVectors, numRows);
  }

  public static long getNativeHandle(ColumnarBatch batch) {
    if (!isIntermediateColumnarBatch(batch)) {
      throw new UnsupportedOperationException("Cannot get native batch handle due to " +
        "input batch is not intermediate Gluten batch");
    }
    IndicatorVector iv = (IndicatorVector) batch.column(0);
    return iv.getNativeHandle();
  }

  public static boolean isIntermediateColumnarBatch(ColumnarBatch batch) {
    if (batch.numCols() == 0) {
      throw new IllegalArgumentException("Cannot decide if a batch that has " +
              "no column is intermediate columnar batch or not");
    }
    ColumnVector col0 = batch.column(0);
    if (!(col0 instanceof IndicatorVector)) {
      return false;
    }
    for (int i = 1; i < batch.numCols(); i++) {
      ColumnVector col = batch.column(i);
      if (!(col instanceof PlaceholderVector)) {
        return false;
      }
    }
    return true;
  }
}
