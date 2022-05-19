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

package io.glutenproject.utils

import io.glutenproject.expression.ArrowConverterUtils
import io.glutenproject.vectorized.ArrowWritableColumnVector
import org.apache.arrow.c.{ArrowArray, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.{Field, Schema}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import java.util

object ArrowAbiUtil {

  def importToArrowRecordBatch(allocator: BufferAllocator,
    cSchema: ArrowSchema, cArray: ArrowArray): ArrowRecordBatch = {
    val vsr = importToVectorSchemaRoot(allocator, cSchema, cArray)
    val unloader = new VectorUnloader(vsr)
    unloader.getRecordBatch
  }

  def importToSparkColumnarBatch(allocator: BufferAllocator,
    cSchema: ArrowSchema, cArray: ArrowArray): ColumnarBatch = {
    val vsr = importToVectorSchemaRoot(allocator, cSchema, cArray)
    toSparkColumnarBatch(vsr)
  }

  private def importToVectorSchemaRoot(allocator: BufferAllocator,
    cSchema: ArrowSchema, cArray: ArrowArray): VectorSchemaRoot = {
    val dictProvider = new CDataDictionaryProvider
    val vsr = Data.importVectorSchemaRoot(allocator, cArray, cSchema, dictProvider)
    try {
      vsr
    } finally {
      dictProvider.close()
      //        vsr.close() // remove this if encountering uaf
    }
  }


  def exportFromSparkColumnarBatch(allocator: BufferAllocator, columnarBatch: ColumnarBatch,
    cSchema: ArrowSchema, cArray: ArrowArray): Unit = {
    val vsr = toVectorSchemaRoot(columnarBatch)
    try {
      Data.exportVectorSchemaRoot(allocator, vsr, new CDataDictionaryProvider(), cArray, cSchema)
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          String.format("error exporting columnar batch with schema: %s, vectors: %s",
            vsr.getSchema, vsr.getFieldVectors), e)
    } finally {
      vsr.close()
    }
  }

  private def toSparkColumnarBatch(vsr: VectorSchemaRoot): ColumnarBatch = {
    val rowCount: Int = vsr.getRowCount
    val vectors: Array[ColumnVector] =
      ArrowWritableColumnVector.loadColumns(rowCount, vsr.getFieldVectors)
        .map(v => v)
    new ColumnarBatch(vectors, rowCount)
  }

  private def toVectorSchemaRoot(batch: ColumnarBatch): VectorSchemaRoot = {
    if (batch.numCols == 0) {
      return VectorSchemaRoot.of()
    }
    val fields = new util.ArrayList[Field](batch.numCols)
    for (i <- 0 until batch.numCols) {
      val col: ColumnVector = batch.column(i)
      fields.add(col.asInstanceOf[ArrowWritableColumnVector].getValueVector.getField)
    }
    val arrowRecordBatch: ArrowRecordBatch = ArrowConverterUtils.createArrowRecordBatch(batch)
    val schema: Schema = new Schema(fields)
    val root: VectorSchemaRoot =
      VectorSchemaRoot.create(schema, SparkMemoryUtils.contextAllocator())
    val loader: VectorLoader = new VectorLoader(root)
    loader.load(arrowRecordBatch)
    root
  }
}
