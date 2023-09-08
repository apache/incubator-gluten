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

import io.glutenproject.exception.GlutenException;
import io.glutenproject.exec.ExecutionCtxs;
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators;
import io.glutenproject.utils.ArrowAbiUtil;
import io.glutenproject.utils.ArrowUtil;
import io.glutenproject.utils.ImplicitClass;
import io.glutenproject.vectorized.ArrowWritableColumnVector;

import com.google.common.base.Preconditions;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ColumnarBatches {
  private static final Field FIELD_COLUMNS;

  static {
    try {
      Field f = ColumnarBatch.class.getDeclaredField("columns");
      f.setAccessible(true);
      FIELD_COLUMNS = f;
    } catch (NoSuchFieldException e) {
      throw new GlutenException(e);
    }
  }

  private ColumnarBatches() {}

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
      throw new GlutenException(e);
    }
  }

  /** Heavy batch: Data is readable from JVM and formatted as Arrow data. */
  public static boolean isHeavyBatch(ColumnarBatch batch) {
    if (batch.numCols() == 0) {
      throw new IllegalArgumentException(
          "Cannot decide if a batch that " + "has no column is Arrow columnar batch or not");
    }
    for (int i = 0; i < batch.numCols(); i++) {
      ColumnVector col = batch.column(i);
      if (!(col instanceof ArrowWritableColumnVector)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Light batch: Data is not readable from JVM, a long int handle (which is a pointer usually) is
   * used to bind the batch to a native side implementation.
   */
  public static boolean isLightBatch(ColumnarBatch batch) {
    if (batch.numCols() == 0) {
      throw new IllegalArgumentException(
          "Cannot decide if a batch that has " + "no column is light columnar batch or not");
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

  /**
   * Ensure the input batch is offloaded as native-based columnar batch (See {@link IndicatorVector}
   * and {@link PlaceholderVector}).
   */
  public static ColumnarBatch ensureOffloaded(BufferAllocator allocator, ColumnarBatch batch) {
    if (ColumnarBatches.isLightBatch(batch)) {
      return batch;
    }
    return offload(allocator, batch);
  }

  /**
   * Ensure the input batch is loaded as Arrow-based Java columnar batch. ABI-based sharing will
   * take place if loading is required, which means when the input batch is not loaded yet.
   */
  public static ColumnarBatch ensureLoaded(BufferAllocator allocator, ColumnarBatch batch) {
    if (batch.numCols() == 0) {
      // No need to load batch if no column.
      return batch;
    }
    if (isHeavyBatch(batch)) {
      return batch;
    }
    return load(allocator, batch);
  }

  private static ColumnarBatch load(BufferAllocator allocator, ColumnarBatch input) {
    if (!ColumnarBatches.isLightBatch(input)) {
      throw new IllegalArgumentException(
          "Input is not light columnar batch. "
              + "Please consider to use vanilla spark's row based input by setting one of the below"
              + " configs: \n"
              + "spark.sql.parquet.enableVectorizedReader=false\n"
              + "spark.sql.inMemoryColumnarStorage.enableVectorizedReader=false\n"
              + "spark.sql.orc.enableVectorizedReader=false\n");
    }
    IndicatorVector iv = (IndicatorVector) input.column(0);
    final long handle = iv.getNativeHandle();
    try (ArrowSchema cSchema = ArrowSchema.allocateNew(allocator);
        ArrowArray cArray = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        CDataDictionaryProvider provider = new CDataDictionaryProvider()) {
      ColumnarBatchJniWrapper.INSTANCE.exportToArrow(
          ExecutionCtxs.contextInstance().getHandle(),
          handle,
          cSchema.memoryAddress(),
          cArray.memoryAddress());

      Data.exportSchema(
          allocator, ArrowUtil.toArrowSchema(cSchema, allocator, provider), provider, arrowSchema);

      ColumnarBatch output =
          ArrowAbiUtil.importToSparkColumnarBatch(allocator, arrowSchema, cArray);

      // Follow gluten input's reference count. This might be optimized using
      // automatic clean-up or once the extensibility of ColumnarBatch is enriched
      IndicatorVector giv = (IndicatorVector) input.column(0);
      ImplicitClass.ArrowColumnarBatchRetainer retainer =
          new ImplicitClass.ArrowColumnarBatchRetainer(output);
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

  private static ColumnarBatch offload(BufferAllocator allocator, ColumnarBatch input) {
    if (!isHeavyBatch(input)) {
      throw new IllegalArgumentException("batch is not Arrow columnar batch");
    }
    final long executionCtxHandle = ExecutionCtxs.contextInstance().getHandle();
    try (ArrowArray cArray = ArrowArray.allocateNew(allocator);
        ArrowSchema cSchema = ArrowSchema.allocateNew(allocator)) {
      ArrowAbiUtil.exportFromSparkColumnarBatch(
          ArrowBufferAllocators.contextInstance(), input, cSchema, cArray);
      long handle =
          ColumnarBatchJniWrapper.INSTANCE.createWithArrowArray(
              executionCtxHandle, cSchema.memoryAddress(), cArray.memoryAddress());
      ColumnarBatch output = ColumnarBatches.create(executionCtxHandle, handle);

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
      final IndicatorVector giv = (IndicatorVector) output.column(0);
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

  public static Iterator<InternalRow> emptyRowIterator(int numRows) {
    final int maxRows = numRows;
    return new Iterator<InternalRow>() {
      int rowId = 0;

      @Override
      public boolean hasNext() {
        return rowId < maxRows;
      }

      @Override
      public InternalRow next() {
        if (rowId >= maxRows) {
          throw new NoSuchElementException();
        }
        rowId++;
        return new UnsafeRow(0);
      }
    };
  }

  public static void close(ColumnarBatch input) {
    ColumnarBatchJniWrapper.INSTANCE.close(
        ColumnarBatches.getExecutionCtxHandle(input), ColumnarBatches.getNativeHandle(input));
  }

  public static void close(long executionCtxHandle, long handle) {
    ColumnarBatchJniWrapper.INSTANCE.close(executionCtxHandle, handle);
  }

  /**
   * Combine multiple columnar batches horizontally, assuming each of them is already offloaded.
   * Otherwise {@link UnsupportedOperationException} will be thrown.
   */
  public static long compose(ColumnarBatch... batches) {
    long[] handles = Arrays.stream(batches).mapToLong(ColumnarBatches::getNativeHandle).toArray();
    // we assume all input batches should be managed by same ExecutionCtx.
    long[] executionCtxHandles =
        Arrays.stream(batches)
            .mapToLong(ColumnarBatches::getExecutionCtxHandle)
            .distinct()
            .toArray();
    Preconditions.checkState(
        executionCtxHandles.length == 1,
        "All input batches should be managed by same ExecutionCtx.");
    return ColumnarBatchJniWrapper.INSTANCE.compose(executionCtxHandles[0], handles);
  }

  public static long numBytes(ColumnarBatch input) {
    return ColumnarBatchJniWrapper.INSTANCE.numBytes(
        ColumnarBatches.getExecutionCtxHandle(input), ColumnarBatches.getNativeHandle(input));
  }

  public static String getType(ColumnarBatch input) {
    return ColumnarBatchJniWrapper.INSTANCE.getType(
        ColumnarBatches.getExecutionCtxHandle(input), ColumnarBatches.getNativeHandle(input));
  }

  public static ColumnarBatch create(long executionCtxHandle, long nativeHandle) {
    final IndicatorVector iv = new IndicatorVector(executionCtxHandle, nativeHandle);
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

  public static void retain(ColumnarBatch b) {
    if (isLightBatch(b)) {
      IndicatorVector iv = (IndicatorVector) b.column(0);
      iv.retain();
      return;
    }
    if (isHeavyBatch(b)) {
      for (int i = 0; i < b.numCols(); i++) {
        ArrowWritableColumnVector col = ((ArrowWritableColumnVector) b.column(i));
        col.retain();
      }
      return;
    }
    throw new IllegalStateException("Unreachable code");
  }

  public static void release(ColumnarBatch b) {
    b.close();
  }

  public static long getNativeHandle(ColumnarBatch batch) {
    if (!isLightBatch(batch)) {
      throw new UnsupportedOperationException(
          "Cannot get native batch handle due to "
              + "input batch is not intermediate Gluten batch");
    }
    IndicatorVector iv = (IndicatorVector) batch.column(0);
    return iv.getNativeHandle();
  }

  public static long getExecutionCtxHandle(ColumnarBatch batch) {
    if (!isLightBatch(batch)) {
      throw new UnsupportedOperationException(
          "Cannot get native batch handle due to "
              + "input batch is not intermediate Gluten batch");
    }
    IndicatorVector iv = (IndicatorVector) batch.column(0);
    return iv.getExecutionCtxHandle();
  }
}
