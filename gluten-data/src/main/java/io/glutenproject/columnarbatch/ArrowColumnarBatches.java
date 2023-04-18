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

import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators;
import io.glutenproject.utils.GlutenArrowAbiUtil;
import io.glutenproject.utils.GlutenArrowUtil;
import io.glutenproject.utils.GlutenImplicitClass;
import io.glutenproject.vectorized.ArrowWritableColumnVector;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.lang.reflect.Field;

public class ArrowColumnarBatches {

  private static final Field FIELD_COLUMNS;

  static {
    try {
      Field f = ColumnarBatch.class.getDeclaredField("columns");
      f.setAccessible(true);
      FIELD_COLUMNS = f;
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  private ArrowColumnarBatches() {

  }

  private static void transferVectors(ColumnarBatch from, ColumnarBatch target) {
    try {
      if (target.numCols() != from.numCols()) {
        throw new IllegalStateException();
      }
      final ColumnVector[] vectors = (ColumnVector[]) FIELD_COLUMNS.get(target);
      for (int i = 0; i < target.numCols(); i++) {
        vectors[i] = from.column(i);
      }
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public static void close(ColumnarBatch input) {
    ColumnarBatchJniWrapper.INSTANCE.close(GlutenColumnarBatches.getNativeHandle(input));
  }

  public static long addColumn(ColumnarBatch input, int index, ColumnarBatch col) {
    return ColumnarBatchJniWrapper.INSTANCE.addColumn(
        GlutenColumnarBatches.getNativeHandle(input), index,
        GlutenColumnarBatches.getNativeHandle(col));
  }

  public static long getBytes(ColumnarBatch input) {
    return ColumnarBatchJniWrapper.INSTANCE.getBytes(GlutenColumnarBatches.getNativeHandle(input));
  }

  public static String getType(ColumnarBatch input) {
    return ColumnarBatchJniWrapper.INSTANCE.getType(GlutenColumnarBatches.getNativeHandle(input));
  }

  public static ColumnarBatch load(BufferAllocator allocator, ColumnarBatch input) {
    if (!GlutenColumnarBatches.isIntermediateColumnarBatch(input)) {
      throw new IllegalArgumentException("input is not intermediate Gluten columnar input. " +
          "Please consider to use vanilla spark's row based input by setting one of the below" +
          " configs: \n" +
          "spark.sql.parquet.enableVectorizedReader=false\n" +
          "spark.sql.inMemoryColumnarStorage.enableVectorizedReader=false\n" +
          "spark.sql.orc.enableVectorizedReader=false\n");
    }
    GlutenIndicatorVector iv = (GlutenIndicatorVector) input.column(0);
    final long handle = iv.getNativeHandle();
    try (ArrowSchema cSchema = ArrowSchema.allocateNew(allocator);
         ArrowArray cArray = ArrowArray.allocateNew(allocator);
         ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
         CDataDictionaryProvider provider = new CDataDictionaryProvider()) {
      ColumnarBatchJniWrapper.INSTANCE.exportToArrow(handle, cSchema.memoryAddress(),
          cArray.memoryAddress());

      Data.exportSchema(allocator,
          GlutenArrowUtil.toArrowSchema(cSchema, allocator, provider), provider, arrowSchema);

      ColumnarBatch output = GlutenArrowAbiUtil.importToSparkColumnarBatch(
          allocator, arrowSchema, cArray);

      // Follow gluten input's reference count. This might be optimized using
      // automatic clean-up or once the extensibility of ColumnarBatch is enriched
      GlutenIndicatorVector giv = (GlutenIndicatorVector) input.column(0);
      GlutenImplicitClass.ArrowColumnarBatchRetainer retainer =
          new GlutenImplicitClass.ArrowColumnarBatchRetainer(output);
      for (long i = 0; i < (giv.refCnt() - 1); i++) {
        retainer.retain();
      }

      // close the input one
      for (long i = 0; i < giv.refCnt(); i++) {
        input.close();
      }

      // populate new vectors to input
      transferVectors(output, input);
      return input;
    }
  }

  public static ColumnarBatch offload(BufferAllocator allocator, ColumnarBatch input) {
    if (!isArrowColumnarBatch(input)) {
      throw new IllegalArgumentException("batch is not Arrow columnar batch");
    }
    try (ArrowArray cArray = ArrowArray.allocateNew(allocator);
         ArrowSchema cSchema = ArrowSchema.allocateNew(allocator)) {
      GlutenArrowAbiUtil.exportFromSparkColumnarBatch(
          ArrowBufferAllocators.contextInstance(), input, cSchema, cArray);
      long handle = ColumnarBatchJniWrapper.INSTANCE.createWithArrowArray(cSchema.memoryAddress(),
          cArray.memoryAddress());
      ColumnarBatch output = GlutenColumnarBatches.create(handle);

      // Follow input's reference count. This might be optimized using
      // automatic clean-up or once the extensibility of ColumnarBatch is enriched
      long refCnt = -1L;
      for (int i = 0; i < input.numCols(); i++) {
        ArrowWritableColumnVector col = ((ArrowWritableColumnVector) input.column(i));
        long colRefCnt = col.refCnt();
        if (refCnt == -1L) {
          refCnt = colRefCnt;
        } else {
          if (colRefCnt != refCnt) {
            throw new IllegalStateException();
          }
        }
      }
      if (refCnt == -1L) {
        throw new IllegalStateException();
      }
      final GlutenIndicatorVector giv = (GlutenIndicatorVector) output.column(0);
      for (long i = 0; i < (refCnt - 1); i++) {
        giv.retain();
      }

      // close the input one
      for (long i = 0; i < refCnt; i++) {
        input.close();
      }

      // populate new vectors to input
      transferVectors(output, input);
      return input;
    }
  }

  /**
   * Ensure the input batch is offloaded as native-based columnar batch
   * (See {@link GlutenIndicatorVector} and {@link GlutenPlaceholderVector}).
   */
  public static ColumnarBatch ensureOffloaded(BufferAllocator allocator, ColumnarBatch batch) {
    if (GlutenColumnarBatches.isIntermediateColumnarBatch(batch)) {
      return batch;
    }
    return offload(allocator, batch);
  }

  /**
   * Ensure the input batch is loaded as Arrow-based Java columnar batch. ABI-based sharing
   * will take place if loading is required, which means when the input batch is not loaded yet.
   */
  public static ColumnarBatch ensureLoaded(BufferAllocator allocator, ColumnarBatch batch) {
    if (batch.numCols() == 0) {
      // No need to load batch if no column.
      return batch;
    }
    if (isArrowColumnarBatch(batch)) {
      return batch;
    }
    return load(allocator, batch);
  }

  public static boolean isArrowColumnarBatch(ColumnarBatch batch) {
    if (batch.numCols() == 0) {
      throw new IllegalArgumentException("Cannot decide if a batch that " +
              "has no column is Arrow columnar batch or not");
    }
    for (int i = 0; i < batch.numCols(); i++) {
      ColumnVector col = batch.column(i);
      if (!(col instanceof ArrowWritableColumnVector)) {
        return false;
      }
    }
    return true;
  }
}
