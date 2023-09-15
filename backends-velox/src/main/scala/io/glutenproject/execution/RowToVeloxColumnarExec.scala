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
package io.glutenproject.execution

import io.glutenproject.backendsapi.velox.Validator
import io.glutenproject.columnarbatch.ColumnarBatches
import io.glutenproject.exec.ExecutionCtxs
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.memory.nmm.NativeMemoryManagers
import io.glutenproject.utils.ArrowAbiUtil
import io.glutenproject.vectorized._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.TaskResources

import org.apache.arrow.c.ArrowSchema
import org.apache.arrow.memory.ArrowBuf

import scala.collection.mutable.ListBuffer

case class RowToVeloxColumnarExec(child: SparkPlan) extends RowToColumnarExecBase(child = child) {

  override def doExecuteColumnarInternal(): RDD[ColumnarBatch] = {
    if (!new Validator().doSchemaValidate(schema)) {
      throw new UnsupportedOperationException(
        s"Input schema contains unsupported type when convert row to columnar, " +
          s"${schema.toString()}")
    }

    val numInputRows = longMetric("numInputRows")
    val numOutputBatches = longMetric("numOutputBatches")
    val convertTime = longMetric("convertTime")
    // Instead of creating a new config we are reusing columnBatchSize. In the future if we do
    // combine with some of the Arrow conversion tools we will need to unify some of the configs.
    val numRows = conf.columnBatchSize
    // This avoids calling `schema` in the RDD closure, so that we don't need to include the entire
    // plan (this) in the closure.
    val localSchema = schema
    child.execute().mapPartitions {
      rowIterator =>
        if (rowIterator.isEmpty) {
          Iterator.empty
        } else {
          val arrowSchema =
            SparkArrowUtil.toArrowSchema(localSchema, SQLConf.get.sessionLocalTimeZone)
          val executionCtxHandle = ExecutionCtxs.contextInstance().getHandle
          val jniWrapper = new NativeRowToColumnarJniWrapper()
          val allocator = ArrowBufferAllocators.contextInstance()
          val cSchema = ArrowSchema.allocateNew(allocator)
          var closed = false
          val r2cHandle =
            try {
              ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
              jniWrapper.init(
                cSchema.memoryAddress(),
                executionCtxHandle,
                NativeMemoryManagers
                  .contextInstance("RowToColumnar")
                  .getNativeInstanceHandle)
            } finally {
              cSchema.close()
            }

          TaskResources.addRecycler(s"RowToColumnar_$r2cHandle", 100) {
            if (!closed) {
              jniWrapper.close(executionCtxHandle, r2cHandle)
              closed = true
            }
          }

          val res: Iterator[ColumnarBatch] = new Iterator[ColumnarBatch] {

            override def hasNext: Boolean = {
              val itHasNext = rowIterator.hasNext
              if (!itHasNext && !closed) {
                jniWrapper.close(executionCtxHandle, r2cHandle)
                closed = true
              }
              itHasNext
            }

            def nativeConvert(row: UnsafeRow): ColumnarBatch = {
              var arrowBuf: ArrowBuf = null
              TaskResources.addRecycler("RowToColumnar_arrowBuf", 100) {
                // Remind, remove isOpen here
                if (arrowBuf != null && arrowBuf.refCnt() != 0) {
                  arrowBuf.close()
                }
              }
              val rowLength = new ListBuffer[Long]()
              var rowCount = 0
              var offset = 0
              val sizeInBytes = row.getSizeInBytes
              // allocate buffer based on 1st row, but if first row is very big, this will cause OOM
              // maybe we should optimize to list ArrayBuf to native to avoid buf close and allocate
              // 31760L origins from BaseVariableWidthVector.lastValueAllocationSizeInBytes
              // experimental value
              val estimatedBufSize = Math.max(
                Math.min(sizeInBytes.toDouble * numRows * 1.2, 31760L * numRows),
                sizeInBytes.toDouble * 10)
              arrowBuf = allocator.buffer(estimatedBufSize.toLong)
              Platform.copyMemory(
                row.getBaseObject,
                row.getBaseOffset,
                null,
                arrowBuf.memoryAddress() + offset,
                sizeInBytes)
              offset += sizeInBytes
              rowLength += sizeInBytes.toLong
              rowCount += 1

              while (rowCount < numRows && rowIterator.hasNext) {
                val row = rowIterator.next()
                val unsafeRow = convertToUnsafeRow(row)
                val sizeInBytes = unsafeRow.getSizeInBytes
                if ((offset + sizeInBytes) > arrowBuf.capacity()) {
                  val tmpBuf = allocator.buffer(((offset + sizeInBytes) * 2).toLong)
                  tmpBuf.setBytes(0, arrowBuf, 0, offset)
                  arrowBuf.close()
                  arrowBuf = tmpBuf
                }
                Platform.copyMemory(
                  unsafeRow.getBaseObject,
                  unsafeRow.getBaseOffset,
                  null,
                  arrowBuf.memoryAddress() + offset,
                  sizeInBytes)
                offset += sizeInBytes
                rowLength += sizeInBytes.toLong
                rowCount += 1
              }
              numInputRows += rowCount
              try {
                val handle = jniWrapper
                  .nativeConvertRowToColumnar(
                    executionCtxHandle,
                    r2cHandle,
                    rowLength.toArray,
                    arrowBuf.memoryAddress())
                ColumnarBatches.create(executionCtxHandle, handle)
              } finally {
                arrowBuf.close()
                arrowBuf = null
              }
            }

            def convertToUnsafeRow(row: InternalRow): UnsafeRow = {
              row match {
                case unsafeRow: UnsafeRow => unsafeRow
                case _ =>
                  val factory = UnsafeProjection
                  val converter = factory.create(localSchema)
                  converter.apply(row)
              }
            }

            override def next(): ColumnarBatch = {
              val firstRow = rowIterator.next()
              val start = System.currentTimeMillis()
              val unsafeRow = convertToUnsafeRow(firstRow)
              val cb = nativeConvert(unsafeRow)
              numOutputBatches += 1
              convertTime += System.currentTimeMillis() - start
              cb
            }
          }
          new CloseableColumnBatchIterator(res)
        }
    }
  }

  // For spark 3.2.
  protected def withNewChildInternal(newChild: SparkPlan): RowToVeloxColumnarExec =
    copy(child = newChild)
}
