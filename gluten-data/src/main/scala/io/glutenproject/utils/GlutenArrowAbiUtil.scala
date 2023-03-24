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

import scala.collection.convert.ImplicitConversions.`seq AsJavaList`

import io.glutenproject.columnarbatch.ArrowColumnarBatches
import io.glutenproject.vectorized.ArrowWritableColumnVector
import org.apache.arrow.c.{ArrowArray, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, VectorLoader, VectorSchemaRoot, VectorUnloader}
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch
import org.apache.arrow.vector.types.pojo.{Field, Schema}

import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

object GlutenArrowAbiUtil {

  def importIntoVectorSchemaRoot(allocator: BufferAllocator,
                                 array: ArrowArray, root: VectorSchemaRoot,
                                 provider: DictionaryProvider): Unit = {
    Data.importIntoVectorSchemaRoot(allocator, array, root, provider)
  }

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

  private def importToVectorSchemaRoot(allocator: BufferAllocator, cSchema: ArrowSchema,
                                       cArray: ArrowArray): VectorSchemaRoot = {
    val dictProvider = new CDataDictionaryProvider
    val vsr = Data.importVectorSchemaRoot(allocator, cArray, cSchema, dictProvider)
    try {
      vsr
    } finally {
      dictProvider.close()
      //        vsr.close() // remove this if encountering uaf
    }
  }

  def importToSparkColumnarBatch(allocator: BufferAllocator,
                                 schema: Schema, cArray: ArrowArray): ColumnarBatch = {
    val vsr = toVectorSchemaRoot(allocator, schema, cArray)
    toSparkColumnarBatch(vsr)
  }

  private def toSparkColumnarBatch(vsr: VectorSchemaRoot): ColumnarBatch = {
    val rowCount: Int = vsr.getRowCount
    val vectors: Array[ColumnVector] =
      ArrowWritableColumnVector.loadColumns(rowCount, vsr.getFieldVectors)
        .map(v => {
          v.setValueCount(rowCount)
          v
        })
    new ColumnarBatch(vectors, rowCount)
  }

  private def toVectorSchemaRoot(allocator: BufferAllocator, schema: Schema, array: ArrowArray)
  : VectorSchemaRoot = {
    val provider = new CDataDictionaryProvider

    val vsr = VectorSchemaRoot.create(schema, allocator);
    try {
      if (array != null) {
        Data.importIntoVectorSchemaRoot(allocator, array, vsr, provider)
      }
      vsr
    } finally {
      provider.close()
    }
  }

  def importToSchema(allocator: BufferAllocator,
                     cSchema: ArrowSchema): Schema = {
    val dictProvider = new CDataDictionaryProvider
    val schema = Data.importSchema(allocator, cSchema, dictProvider)
    try {
      schema
    } finally {
      dictProvider.close()
    }
  }

  def exportField(allocator: BufferAllocator, field: Field, out: ArrowSchema) {
    val dictProvider = new CDataDictionaryProvider
    try {
      Data.exportField(allocator, field, dictProvider, out)
    } finally {
      dictProvider.close()
    }
  }

  def exportSchema(allocator: BufferAllocator, schema: Schema, out: ArrowSchema) {
    val dictProvider = new CDataDictionaryProvider
    try {
      Data.exportSchema(allocator, schema, dictProvider, out)
    } finally {
      dictProvider.close()
    }
  }

  def exportFromSparkColumnarBatch(allocator: BufferAllocator, columnarBatch: ColumnarBatch,
                                   cSchema: ArrowSchema, cArray: ArrowArray): Unit = {
    val loaded = ArrowColumnarBatches.ensureLoaded(allocator, columnarBatch)
    val schema = GlutenArrowUtil.toSchema(loaded)
    val rb = GlutenArrowUtil.createArrowRecordBatch(loaded)
    try {
      exportFromArrowRecordBatch(allocator, rb, schema, cSchema, cArray)
    } finally {
      GlutenArrowUtil.releaseArrowRecordBatch(rb)
    }
  }

  def exportFromArrowRecordBatch(allocator: BufferAllocator, arrowBatch: ArrowRecordBatch,
                                 schema: Schema, cSchema: ArrowSchema, cArray: ArrowArray)
  : Unit = {
    val vsr = toVectorSchemaRoot(allocator, schema, arrowBatch)
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

  // will release input record batch
  private def toVectorSchemaRoot(allocator: BufferAllocator, schema: Schema,
                                 arrowBatch: ArrowRecordBatch)
  : VectorSchemaRoot = {
    if (arrowBatch.getNodes.size() == 0) {
      return VectorSchemaRoot.of()
    }
    val root: VectorSchemaRoot =
      VectorSchemaRoot.create(schema, allocator)
    val loader: VectorLoader = new VectorLoader(root)
    loader.load(arrowBatch)
    root
  }

  private def toVectorSchemaRoot(schema: Schema, fieldVectors: List[FieldVector])
  : VectorSchemaRoot = {
    val rowCount = if (fieldVectors.isEmpty) 0
    else fieldVectors.get(0).getValueCount
    new VectorSchemaRoot(schema, fieldVectors, rowCount)
  }
}
