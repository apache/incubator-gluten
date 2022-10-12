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

package io.glutenproject.vectorized

import java.io._
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

import io.glutenproject.GlutenConfig
import io.glutenproject.utils.ArrowAbiUtil
import org.apache.arrow.c.{ArrowArray, ArrowSchema}
import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.{VectorLoader, VectorSchemaRoot}
import org.apache.arrow.vector.compression.NoCompressionCodec
import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.arrow.vector.types.pojo.Schema

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.execution.datasources.v2.arrow.{SparkMemoryUtils, SparkVectorUtils}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

class ArrowColumnarBatchSerializer(schema: StructType, readBatchNumRows: SQLMetric,
                                   numOutputRows: SQLMetric)
  extends Serializer with Serializable {

  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance = {
    val arrowSchema = ArrowUtils.toArrowSchema(schema, SQLConf.get.sessionLocalTimeZone)
    new ArrowColumnarBatchSerializerInstance(arrowSchema, readBatchNumRows, numOutputRows)
  }
}

private class ArrowColumnarBatchSerializerInstance(
                                                    schema: Schema,
                                                    readBatchNumRows: SQLMetric,
                                                    numOutputRows: SQLMetric)
  extends SerializerInstance
    with Logging {

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {

      private val readSchema = GlutenConfig.getConf.columnarShuffleWriteSchema

      private val compressionEnabled =
        SparkEnv.get.conf.getBoolean("spark.shuffle.compress", true)

      private val allocator: BufferAllocator = SparkMemoryUtils
        .contextArrowAllocator()
        .newChildAllocator("ArrowColumnarBatch deserialize", 0, Long.MaxValue)

      private val reader = new SchemaAwareArrowStreamReader(
        if (readSchema) null else schema, in, allocator)
      private var root: VectorSchemaRoot = _
      private var vectors: Array[ColumnVector] = _
      private var cb: ColumnarBatch = _
      private var batchLoaded = true

      private var jniWrapper: ShuffleDecompressionJniWrapper = _
      private var schemaHolderId: Long = 0
      private var vectorLoader: VectorLoader = _

      private var numBatchesTotal: Long = _
      private var numRowsTotal: Long = _

      private var isClosed: Boolean = false

      override def asIterator: Iterator[Any] = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def readKey[T: ClassTag](): T = {
        // We skipped serialization of the key in writeKey(), so just return a dummy value since
        // this is going to be discarded anyways.
        null.asInstanceOf[T]
      }

      @throws(classOf[EOFException])
      override def readValue[T: ClassTag](): T = {
        if (cb != null) {
          cb.close()
          cb = null
        }
        try {
          batchLoaded = reader.loadNextBatch()
        } catch {
          case ioe: IOException =>
            this.close()
            logError("Failed to load next RecordBatch", ioe)
            throw ioe
        }
        if (!batchLoaded) {
          this.close()
          throw new EOFException
        }
        root = reader.getVectorSchemaRoot
        val numRows = root.getRowCount
        logDebug(s"Read ColumnarBatch of ${numRows} rows")
        numBatchesTotal += 1
        numRowsTotal += numRows
        if (compressionEnabled &&
          !reader.getBatchCompressType.equals(CompressType.NO_COMPRESSION)) {
          try {
            decompressVectors()
          } catch {
            case e: UnsupportedOperationException =>
              this.close()
              throw e
          }
        }
        val newFieldVectors = root.getFieldVectors.asScala.map { vector =>
          val newVector = vector.getField.createVector(allocator)
          vector.makeTransferPair(newVector).transfer()
          newVector
        }.asJava
        vectors = ArrowWritableColumnVector
          .loadColumns(numRows, newFieldVectors)
          .toArray[ColumnVector]

        cb = new ColumnarBatch(vectors, numRows)
        cb.asInstanceOf[T]
      }

      override def readObject[T: ClassTag](): T = {
        // This method is never called by shuffle code.
        throw new UnsupportedOperationException
      }

      override def close(): Unit = {
        if (!isClosed) {
          if (numBatchesTotal > 0) {
            readBatchNumRows.set(numRowsTotal.toDouble / numBatchesTotal)
          }
          numOutputRows += numRowsTotal
          if (cb != null) cb.close()
          if (reader != null) reader.close(true)
          if (jniWrapper != null) jniWrapper.close(schemaHolderId)
          isClosed = true
        }
      }

      private def decompressVectors(): Unit = {
        if (jniWrapper == null) {
          jniWrapper = new ShuffleDecompressionJniWrapper
          val out = ArrowSchema.allocateNew(SparkMemoryUtils.contextArrowAllocator())
          try {
            ArrowAbiUtil.exportSchema(SparkMemoryUtils.contextArrowAllocator(), root.getSchema, out)
            schemaHolderId = jniWrapper.make(out.memoryAddress())
          } finally {
            out.close()
          }
        }
        if (vectorLoader == null) {
          vectorLoader = new VectorLoader(root)
        }
        val bufAddrs = new ListBuffer[Long]()
        val bufSizes = new ListBuffer[Long]()
        val bufBS = mutable.BitSet()
        var bufIdx = 0

        root.getFieldVectors.asScala.foreach { vector =>
          val buffers = new java.util.ArrayList[ArrowBuf]()
          val bits = new java.util.ArrayList[Boolean]()
          SparkVectorUtils.appendNodes(vector, null, buffers, bits);
          (buffers.asScala zip bits.asScala).foreach {
            case (buffer, is_bit) =>
              if (is_bit) {
                val validityBuf = buffer
                if (validityBuf
                  .capacity() <= 8 || java.lang.Long.bitCount(validityBuf.getLong(0)) == 64
                  || java.lang.Long.bitCount(validityBuf.getLong(0)) == 0) {
                  bufBS.add(bufIdx)
                }
              }

              bufAddrs += buffer.memoryAddress()
              // buffer.readableBytes() will return wrong readable length here
              // since it is initialized by
              // data stored in IPC message header, which is not the actual compressed length
              bufSizes += buffer.capacity()
              bufIdx += 1
          }
        }
        val cArray = ArrowArray.allocateNew(allocator)
        val cSchema = ArrowSchema.allocateNew(allocator)

        try {
          val open = jniWrapper.decompress(
            schemaHolderId,
            reader.getBatchCompressType.getAlias,
            root.getRowCount,
            bufAddrs.toArray,
            bufSizes.toArray,
            bufBS.toBitMask,
            cSchema.memoryAddress(),
            cArray.memoryAddress())

          if (!open) {
            // expected an error before reaching this line
            throw new IllegalStateException()
          }

          val decompressedRecordBatch =
            ArrowAbiUtil.importToArrowRecordBatch(allocator, cSchema, cArray)

          root.clear()
          if (decompressedRecordBatch != null) {
            vectorLoader.load(decompressedRecordBatch)
            logDebug(
              s"ArrowColumnarBatchSerializer, Decompressed vector is " +
                s"${root.contentToTSVString()}")
            decompressedRecordBatch.close()
          }
        } finally {
          cArray.close()
          cSchema.close()
        }
      }
    }
  }

  // Columnar shuffle write process don't need this.
  override def serializeStream(s: OutputStream): SerializationStream =
    throw new UnsupportedOperationException

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException
}
