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

import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.unsafe.types.UTF8String;

public class ConvertColumnVector {

  public static void convert(OffHeapColumnVector currentVector,
                             int numCols,
                             int numRows) {
    if (currentVector.hasDictionary()) {
      if (currentVector.dataType() instanceof IntegerType) {
        int[] values = new int[numRows];
        for (int i = 0; i < numRows; i++) {
          values[i] = currentVector.getInt(i);
        }
      } else if (currentVector.dataType() instanceof LongType) {
        long[] values = new long[numRows];
        for (int i = 0; i < numRows; i++) {
          values[i] = currentVector.getLong(i);
        }
      } else if (currentVector.dataType() instanceof DoubleType) {
        double[] values = new double[numRows];
        for (int i = 0; i < numRows; i++) {
          values[i] = currentVector.getDouble(i);
        }
      } else if (currentVector.dataType() instanceof DateType) {
        int[] values = new int[numRows];
        for (int i = 0; i < numRows; i++) {
          values[i] = currentVector.getInt(i);
        }
      } else if (currentVector.dataType() instanceof StringType) {
        UTF8String[] values = new UTF8String[numRows];
        for (int i = 0; i < numRows; i++) {
          values[i] = currentVector.getUTF8String(i);
        }
      }
    } else {
      if (currentVector.dataType() instanceof IntegerType) {
        int[] values = currentVector.getInts(0, numRows);
      } else if (currentVector.dataType() instanceof LongType) {
        long[] values = currentVector.getLongs(0, numRows);
      } else if (currentVector.dataType() instanceof DoubleType) {
        double[] values = currentVector.getDoubles(0, numRows);
      } else if (currentVector.dataType() instanceof DateType) {
        int[] values = currentVector.getInts(0, numRows);
      } else if (currentVector.dataType() instanceof StringType) {
        UTF8String[] values = new UTF8String[numRows];
        for (int i = 0; i < numRows; i++) {
          values[i] = currentVector.getUTF8String(i);
        }
      }
    }
  }
}
