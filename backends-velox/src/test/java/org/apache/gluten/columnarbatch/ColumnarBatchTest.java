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
package org.apache.gluten.columnarbatch;

import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators;
import org.apache.gluten.test.VeloxBackendTestBase;
import org.apache.gluten.vectorized.ArrowWritableColumnVector;

import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.TaskResources$;
import org.junit.Assert;
import org.junit.Test;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public class ColumnarBatchTest extends VeloxBackendTestBase {

  @Test
  public void testOffloadAndLoad() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final int numRows = 100;
          final ColumnarBatch batch = newArrowBatch("a boolean, b int", numRows);
          Assert.assertTrue(ColumnarBatches.isHeavyBatch(batch));
          final ColumnarBatch offloaded =
              ColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance(), batch);
          Assert.assertTrue(ColumnarBatches.isLightBatch(offloaded));
          final ColumnarBatch loaded =
              ColumnarBatches.ensureLoaded(ArrowBufferAllocators.contextInstance(), offloaded);
          Assert.assertTrue(ColumnarBatches.isHeavyBatch(loaded));
          long cnt =
              StreamSupport.stream(
                      Spliterators.spliteratorUnknownSize(
                          loaded.rowIterator(), Spliterator.ORDERED),
                      false)
                  .count();
          Assert.assertEquals(numRows, cnt);
          loaded.close();
          return null;
        });
  }

  @Test
  public void testCreateByHandle() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final int numRows = 100;
          final ColumnarBatch batch = newArrowBatch("a boolean, b int", numRows);
          Assert.assertEquals(1, ColumnarBatches.getRefCnt(batch));
          final ColumnarBatch offloaded =
              ColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance(), batch);
          Assert.assertEquals(1, ColumnarBatches.getRefCnt(offloaded));
          final long handle = ColumnarBatches.getNativeHandle(offloaded);
          final ColumnarBatch created = ColumnarBatches.create(handle);
          Assert.assertEquals(handle, ColumnarBatches.getNativeHandle(created));
          Assert.assertEquals(1, ColumnarBatches.getRefCnt(offloaded));
          Assert.assertEquals(1, ColumnarBatches.getRefCnt(created));
          ColumnarBatches.retain(created);
          Assert.assertEquals(2, ColumnarBatches.getRefCnt(offloaded));
          Assert.assertEquals(2, ColumnarBatches.getRefCnt(created));
          ColumnarBatches.retain(offloaded);
          Assert.assertEquals(3, ColumnarBatches.getRefCnt(offloaded));
          Assert.assertEquals(3, ColumnarBatches.getRefCnt(created));
          created.close();
          Assert.assertEquals(2, ColumnarBatches.getRefCnt(offloaded));
          Assert.assertEquals(2, ColumnarBatches.getRefCnt(created));
          offloaded.close();
          Assert.assertEquals(1, ColumnarBatches.getRefCnt(offloaded));
          Assert.assertEquals(1, ColumnarBatches.getRefCnt(created));
          created.close();
          Assert.assertEquals(0, ColumnarBatches.getRefCnt(offloaded));
          Assert.assertEquals(0, ColumnarBatches.getRefCnt(created));
          return null;
        });
  }

  @Test
  public void testOffloadAndLoadReadRow() {
    TaskResources$.MODULE$.runUnsafe(
        () -> {
          final int numRows = 100;
          final ColumnarBatch batch = newArrowBatch("a boolean, b int", numRows);
          final ArrowWritableColumnVector col0 = (ArrowWritableColumnVector) batch.column(0);
          final ArrowWritableColumnVector col1 = (ArrowWritableColumnVector) batch.column(1);
          for (int j = 0; j < numRows; j++) {
            col0.putBoolean(j, j % 2 == 0);
            col1.putInt(j, 15 - j);
          }
          col1.putNull(numRows - 1);
          Assert.assertTrue(ColumnarBatches.isHeavyBatch(batch));
          final ColumnarBatch offloaded =
              ColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance(), batch);
          Assert.assertTrue(ColumnarBatches.isLightBatch(offloaded));
          final ColumnarBatch loaded =
              ColumnarBatches.ensureLoaded(ArrowBufferAllocators.contextInstance(), offloaded);
          Assert.assertTrue(ColumnarBatches.isHeavyBatch(loaded));
          long cnt =
              StreamSupport.stream(
                      Spliterators.spliteratorUnknownSize(
                          loaded.rowIterator(), Spliterator.ORDERED),
                      false)
                  .count();
          Assert.assertEquals(numRows, cnt);
          Assert.assertEquals(loaded.getRow(0).getInt(1), 15);
          loaded.close();
          return null;
        });
  }

  private static ColumnarBatch newArrowBatch(String schema, int numRows) {
    final ArrowWritableColumnVector[] columns =
        ArrowWritableColumnVector.allocateColumns(numRows, StructType.fromDDL(schema));
    for (ArrowWritableColumnVector col : columns) {
      col.setValueCount(numRows);
    }
    final ColumnarBatch batch = new ColumnarBatch(columns);
    batch.setNumRows(numRows);
    return batch;
  }
}
