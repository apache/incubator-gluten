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

public class GlutenColumnarBatches {
  public ColumnarBatch create(long nativeHandle) {
    final GlutenIndicatorVector iv = new GlutenIndicatorVector(nativeHandle);
    int numColumns = Math.toIntExact(iv.getNumColumns());
    int numRows = Math.toIntExact(iv.getNumRows());
    if (numColumns == 0) {
      return new ColumnarBatch(new ColumnVector[0], numRows);
    }
    final ColumnVector[] columnVectors = new ColumnVector[numColumns];
    columnVectors[0] = iv;
    long numPlaceholders = numColumns - 1;
    for (int i = 0; i < numPlaceholders; i++) {
      final GlutenPlaceholderVector pv = new GlutenPlaceholderVector();
      columnVectors[i + 1] = pv;
    }
    return new ColumnarBatch(columnVectors, numRows);
  }
}
