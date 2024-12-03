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
package org.apache.spark.sql.execution.unsafe

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.memory.arrow.alloc.ArrowBufferAllocators
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.utils.ArrowAbiUtil
import org.apache.gluten.vectorized.{ColumnarBatchSerializerJniWrapper, NativeColumnarToRowJniWrapper}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.{TaskMemoryManager, UnifiedMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.joins.BuildSideRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.task.TaskResources
import org.apache.spark.util.Utils

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.arrow.c.ArrowSchema

import java.io.{Externalizable, ObjectInput, ObjectOutput}

import scala.collection.JavaConverters.asScalaIteratorConverter

/**
 * UnsafeColumnarBuildSideRelation should backed by offheap to avoid on-heap oom. Almost the same as
 * ColumnarBuildSideRelation, we should remove ColumnarBuildSideRelation when
 * UnsafeColumnarBuildSideRelation get matured.
 *
 * @param output
 * @param batches
 */
case class UnsafeColumnarBuildSideRelation(
    private var output: Seq[Attribute],
    private var batches: UnsafeBytesBufferArray)
  extends BuildSideRelation
  with Externalizable
  with Logging
  with KryoSerializable {

  def this(output: Seq[Attribute], bytesBufferArray: Array[Array[Byte]]) {
    // only used in driver side when broadcast the whole batches
    this(
      output,
      UnsafeBytesBufferArray(
        bytesBufferArray.length,
        bytesBufferArray.map(_.length),
        bytesBufferArray.map(_.length.toLong).sum,
        TaskContext.get().taskMemoryManager
      )
    )
    val batchesSize = bytesBufferArray.length
    for (i <- 0 until batchesSize) {
      val length = bytesBufferArray(i).length
      log.debug(s"this $i--- $length")
      batches.putBytesBuffer(i, bytesBufferArray(i))
    }
  }

  // should only be used on driver to serialize this relation
  override def writeExternal(out: ObjectOutput): Unit = Utils.tryOrIOException {
    out.writeObject(output)
    out.writeInt(batches.arraySize)
    out.writeObject(batches.bytesBufferLengths)
    out.writeLong(batches.totalBytes)
    for (i <- 0 until batches.arraySize) {
      val bytes = batches.getBytesBuffer(i)
      out.write(bytes)
      log.debug(s"writeExternal index $i with length ${bytes.length}")
    }
  }

  // should only be used on driver to serialize this relation
  override def write(kryo: Kryo, out: Output): Unit = Utils.tryOrIOException {
    kryo.writeObject(out, output.toList)
    out.writeInt(batches.arraySize)
    kryo.writeObject(out, batches.bytesBufferLengths)
    out.writeLong(batches.totalBytes)
    for (i <- 0 until batches.arraySize) {
      val bytes = batches.getBytesBuffer(i)
      out.write(bytes)
      log.debug(s"write index $i with length ${bytes.length}")
    }
  }

  // should only be used on executor to deserialize this relation
  override def readExternal(in: ObjectInput): Unit = Utils.tryOrIOException {
    output = in.readObject().asInstanceOf[Seq[Attribute]]
    val totalArraySize = in.readInt()
    val bytesBufferLengths = in.readObject().asInstanceOf[Array[Int]]
    val totalBytes = in.readLong()

    val taskMemoryManager = new TaskMemoryManager(
      new UnifiedMemoryManager(SparkEnv.get.conf, Long.MaxValue, Long.MaxValue / 2, 1),
      0)

    batches =
      UnsafeBytesBufferArray(totalArraySize, bytesBufferLengths, totalBytes, taskMemoryManager)

    for (i <- 0 until totalArraySize) {
      val length = bytesBufferLengths(i)
      log.debug(s"readExternal $i--- ${bytesBufferLengths(i)}")
      val tmpBuffer = new Array[Byte](length)
      in.read(tmpBuffer)
      batches.putBytesBuffer(i, tmpBuffer)
    }
  }

  override def read(kryo: Kryo, in: Input): Unit = Utils.tryOrIOException {
    output = kryo.readObject(in, classOf[List[_]]).asInstanceOf[Seq[Attribute]]
    val totalArraySize = in.readInt()
    val bytesBufferLengths = kryo.readObject(in, classOf[Array[Int]])
    val totalBytes = in.readLong()

    val taskMemoryManager = new TaskMemoryManager(
      new UnifiedMemoryManager(SparkEnv.get.conf, Long.MaxValue, Long.MaxValue / 2, 1),
      0)

    batches =
      UnsafeBytesBufferArray(totalArraySize, bytesBufferLengths, totalBytes, taskMemoryManager)

    for (i <- 0 until totalArraySize) {
      val length = bytesBufferLengths(i)
      log.debug(s"readExternal $i--- $length")
      val tmpBuffer = new Array[Byte](length)
      in.read(tmpBuffer)
      batches.putBytesBuffer(i, tmpBuffer)
    }
  }

  override def deserialized: Iterator[ColumnarBatch] = {
    val runtime = Runtimes.contextInstance("UnsafeBuildSideRelation#deserialized")
    val jniWrapper = ColumnarBatchSerializerJniWrapper.create(runtime)
    val serializeHandle: Long = {
      val allocator = ArrowBufferAllocators.contextInstance()
      val cSchema = ArrowSchema.allocateNew(allocator)
      val arrowSchema = SparkArrowUtil.toArrowSchema(
        SparkShimLoader.getSparkShims.structFromAttributes(output),
        SQLConf.get.sessionLocalTimeZone)
      ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
      val handle = jniWrapper
        .init(cSchema.memoryAddress())
      cSchema.close()
      handle
    }

    Iterators
      .wrap(new Iterator[ColumnarBatch] {
        var batchId = 0

        override def hasNext: Boolean = {
          batchId < batches.arraySize
        }

        override def next: ColumnarBatch = {
          val (offset, length) =
            batches.getBytesBufferOffsetAndLength(batchId)
          batchId += 1
          val handle =
            jniWrapper.deserialize(serializeHandle, offset, length)
          ColumnarBatches.create(handle)
        }
      })
      .protectInvocationFlow()
      .recycleIterator {
        jniWrapper.close(serializeHandle)
      }
      .recyclePayload(ColumnarBatches.forceClose) // FIXME why force close?
      .create()
  }

  override def asReadOnlyCopy(): UnsafeColumnarBuildSideRelation = this

  /**
   * Transform columnar broadcast value to Array[InternalRow] by key and distinct. NOTE: This method
   * was called in Spark Driver, should manage resources carefully.
   */
  override def transform(key: Expression): Array[InternalRow] = TaskResources.runUnsafe {
    val runtime = Runtimes.contextInstance("BuildSideRelation#transform")
    // This transformation happens in Spark driver, thus resources can not be managed automatically.
    val serializerJniWrapper = ColumnarBatchSerializerJniWrapper.create(runtime)
    val serializeHandle = {
      val allocator = ArrowBufferAllocators.contextInstance()
      val cSchema = ArrowSchema.allocateNew(allocator)
      val arrowSchema = SparkArrowUtil.toArrowSchema(
        SparkShimLoader.getSparkShims.structFromAttributes(output),
        SQLConf.get.sessionLocalTimeZone)
      ArrowAbiUtil.exportSchema(allocator, arrowSchema, cSchema)
      val handle = serializerJniWrapper.init(cSchema.memoryAddress())
      cSchema.close()
      handle
    }

    var closed = false

    val exprIds = output.map(_.exprId)
    val projExpr = key.transformDown {
      case attr: AttributeReference if !exprIds.contains(attr.exprId) =>
        val i = output.count(_.name == attr.name)
        if (i != 1) {
          throw new IllegalArgumentException(s"Only one attr with the same name is supported: $key")
        } else {
          output.find(_.name == attr.name).get
        }
    }
    val proj = UnsafeProjection.create(Seq(projExpr), output)

    // Convert columnar to Row.
    val jniWrapper = NativeColumnarToRowJniWrapper.create(runtime)
    val c2rId = jniWrapper.nativeColumnarToRowInit()
    var batchId = 0
    val iterator = if (batches.arraySize > 0) {
      val res: Iterator[Iterator[InternalRow]] = new Iterator[Iterator[InternalRow]] {
        override def hasNext: Boolean = {
          val itHasNext = batchId < batches.arraySize
          if (!itHasNext && !closed) {
            jniWrapper.nativeClose(c2rId)
            serializerJniWrapper.close(serializeHandle)
            closed = true
          }
          itHasNext
        }

        override def next(): Iterator[InternalRow] = {
          val (offset, length) = batches.getBytesBufferOffsetAndLength(batchId)
          batchId += 1
          val batchHandle =
            serializerJniWrapper.deserialize(serializeHandle, offset, length)
          val batch = ColumnarBatches.create(batchHandle)
          if (batch.numRows == 0) {
            batch.close()
            Iterator.empty
          } else if (output.isEmpty) {
            val rows = ColumnarBatches.emptyRowIterator(batch.numRows()).asScala
            batch.close()
            rows
          } else {
            val cols = batch.numCols()
            val rows = batch.numRows()
            var info =
              jniWrapper.nativeColumnarToRowConvert(
                c2rId,
                ColumnarBatches.getNativeHandle(batch),
                0)
            batch.close()

            new Iterator[InternalRow] {
              var rowId = 0
              var baseLength = 0
              val row = new UnsafeRow(cols)

              override def hasNext: Boolean = {
                rowId < rows
              }

              override def next: UnsafeRow = {
                if (rowId >= rows) throw new NoSuchElementException
                if (rowId == baseLength + info.lengths.length) {
                  baseLength += info.lengths.length
                  info = jniWrapper.nativeColumnarToRowConvert(batchHandle, c2rId, rowId)
                }
                val (offset, length) =
                  (info.offsets(rowId - baseLength), info.lengths(rowId - baseLength))
                row.pointTo(null, info.memoryAddress + offset, length.toInt)
                rowId += 1
                row
              }
            }.map(proj).map(_.copy())
          }
        }
      }
      res.flatten
    } else {
      Iterator.empty
    }
    iterator.toArray
  }
}
