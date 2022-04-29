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

package io.glutenproject.execution;

import io.glutenproject.vectorized.CHColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.util.Iterator;

public class ColumnarNativeIterator extends AbstractColumnarNativeIterator {

  public ColumnarNativeIterator(Iterator<ColumnarBatch> delegated) {
    super(delegated);
  }

  @Override
  public byte[] next() {
    ColumnarBatch dep_cb = nextBatch;
    if (dep_cb.numRows() > 0) {
      CHColumnVector col = (CHColumnVector) dep_cb.column(0);
      return longtoBytes(col.getBlockAddress());
    } else {
      throw new IllegalStateException();
    }
  }

  private static byte[] longtoBytes(long data) {
    return new byte[]{
        (byte) ((data >> 56) & 0xff),
        (byte) ((data >> 48) & 0xff),
        (byte) ((data >> 40) & 0xff),
        (byte) ((data >> 32) & 0xff),
        (byte) ((data >> 24) & 0xff),
        (byte) ((data >> 16) & 0xff),
        (byte) ((data >> 8) & 0xff),
        (byte) ((data >> 0) & 0xff),
    };
  }
}
