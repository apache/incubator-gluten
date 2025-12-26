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
package org.apache.gluten.vectorized;

import org.apache.spark.sql.execution.vectorized.ColumnarArrayShim;
import org.apache.spark.sql.vectorized.ColumnVector;

/**
 * Because `get` method in `ColumnarArray` don't check whether the data to get is null and arrow
 * vectors will throw exception when we try to access null value, so we define the following class
 * as a workaround. Its implementation is copied from Spark-4.0, except that the `handleNull`
 * parameter is set to true when we call `SpecializedGettersReader.read` in `get`, which means that
 * when trying to access a value of the array, we will check whether the value to get is null first.
 *
 * <p>The actual implementation is put in [[ColumnarArrayShim]] because Variant data type is
 * introduced in Spark-4.0.
 */
public class ArrowColumnarArray extends ColumnarArrayShim {
  public ArrowColumnarArray(ColumnVector data, int offset, int length) {
    super(data, offset, length);
  }
}
