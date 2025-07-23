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
package org.apache.gluten.streaming.api.operators.utils;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class RowDataTestUtils {

  public static void checkEquals(RowData actual, RowData expected, List<LogicalType> fieldTypes) {
    assertEquals("Row arity mismatch", expected.getArity(), actual.getArity());
    assertEquals("Field types count mismatch", fieldTypes.size(), actual.getArity());

    for (int i = 0; i < actual.getArity(); i++) {
      RowData.FieldGetter getter = RowData.createFieldGetter(fieldTypes.get(i), i);
      Object actualValue = getter.getFieldOrNull(actual);
      Object expectedValue = getter.getFieldOrNull(expected);

      assertEquals("Field " + i + " mismatch", expectedValue, actualValue);
    }
  }

  public static void checkEquals(
      List<RowData> actual, List<RowData> expected, List<LogicalType> filedTypes) {
    assertEquals("List size mismatch", expected.size(), actual.size());

    for (int i = 0; i < expected.size(); i++) {
      checkEquals(actual.get(i), expected.get(i), filedTypes);
    }
  }
}
