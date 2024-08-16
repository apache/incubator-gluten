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

public class ColumnarBatchRowUtil {

  private static final Field FIELD_COLUMNAR_BATCH_ROW;

  static {
    try {
      Field row = ColumnarBatch.class.getDeclaredField("row");
      row.setAccessible(true);
      FIELD_COLUMNAR_BATCH_ROW = row;
    } catch (NoSuchFieldException e) {
      throw new GlutenException(e);
    }
  }

  public static void setColumnarBatchRow(ColumnVector[] columns, ColumnarBatch target) {
    ColumnarBatchRow row = new ColumnarBatchRow(columns);
    try {
      FIELD_COLUMNAR_BATCH_ROW.set(target, row);
    } catch (IllegalAccessException e) {
      throw new GlutenException(e);
    }
  }
}
