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

import java.io._
import java.nio.channels.Channels
import java.util

import scala.collection.JavaConverters._

import io.glutenproject.columnarbatch.ArrowColumnarBatches
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.vectorized.ArrowWritableColumnVector
import io.netty.buffer.{ByteBufAllocator, ByteBufOutputStream}
import org.apache.arrow.c.{ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.ValueVector
import org.apache.arrow.vector.ipc.{ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.v2.arrow.{SparkSchemaUtil, SparkVectorUtil}
import org.apache.spark.sql.types._
import org.apache.spark.sql.utils.SparkArrowUtil
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

object GlutenArrowUtil extends Logging {

  private val defaultTimeZoneId = SparkSchemaUtil.getLocalTimezoneID()

  def convertToNetty(iter: Array[ColumnarBatch]): Array[Byte] = {
    val innerBuf = ByteBufAllocator.DEFAULT.buffer()
    val outStream = new ByteBufOutputStream(innerBuf)
    convertToNetty(iter, outStream)
    val bytes = new Array[Byte](innerBuf.readableBytes);
    innerBuf.getBytes(innerBuf.readerIndex, bytes);
    innerBuf.release()
    outStream.close()
    bytes
  }

  def convertToNetty(iter: Array[ColumnarBatch], out: OutputStream): Unit = {
    val channel = new WriteChannel(Channels.newChannel(out))
    var schema: Schema = null
    val option = new IpcOption

    iter.foreach {
      columnarBatch =>
        val vectors = (0 until columnarBatch.numCols)
          .map(
            i =>
              ArrowColumnarBatches
                .ensureLoaded(ArrowBufferAllocators.contextInstance(), columnarBatch)
                .column(i)
                .asInstanceOf[ArrowWritableColumnVector])
          .toList
        try {
          if (schema == null) {
            schema = new Schema(vectors.map(_.getValueVector().getField).asJava)
            MessageSerializer.serialize(channel, schema, option)
          }
          val batch = GlutenArrowUtil
            .createArrowRecordBatch(columnarBatch.numRows, vectors.map(_.getValueVector))
          try {
            MessageSerializer.serialize(channel, batch, option)
          } finally {
            batch.close()
          }
        } catch {
          case e =>
            // scalastyle:off println
            System.err.println(s"Failed converting to Netty. ")
            e.printStackTrace()
            // scalastyle:on println
            throw e
        }
    }
  }

  def createArrowRecordBatch(numRowsInBatch: Int, cols: List[ValueVector]): ArrowRecordBatch = {
    SparkVectorUtil.toArrowRecordBatch(numRowsInBatch, cols)
  }

  def convertFromNetty(
      attributes: Seq[Attribute],
      data: Array[Array[Byte]],
      columnIndices: Array[Int] = null): Iterator[ColumnarBatch] = {
    if (data.length == 0) {
      return new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = false

        override def next(): ColumnarBatch = {
          val resultStructType = if (columnIndices == null) {
            StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
          } else {
            StructType(
              columnIndices
                .map(i => attributes(i))
                .map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
          }
          val resultColumnVectors =
            ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
          return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
        }
      }
    }
    new Iterator[ColumnarBatch] {
      var array_id = 0
      val allocator = ArrowBufferAllocators.contextInstance()
      var input = new ByteArrayInputStream(data(array_id))
      var messageReader =
        new MessageChannelReader(new ReadChannel(Channels.newChannel(input)), allocator)
      var schema: Schema = null
      var result: MessageResult = null

      override def hasNext: Boolean =
        if (array_id < (data.length - 1) || input.available > 0) {
          true
        } else {
          messageReader.close()
          false
        }

      override def next(): ColumnarBatch = {
        if (input.available == 0) {
          messageReader.close()
          array_id += 1
          input = new ByteArrayInputStream(data(array_id))
          messageReader =
            new MessageChannelReader(new ReadChannel(Channels.newChannel(input)), allocator)
        }
        if (input.available == 0) {
          val resultStructType = StructType(
            attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
          val resultColumnVectors =
            ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
          return new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
        }
        try {
          if (schema == null) {
            result = messageReader.readNext();

            if (result == null) {
              throw new IOException("Unexpected end of input. Missing schema.");
            }

            if (result.getMessage.headerType() != MessageHeader.Schema) {
              throw new IOException(
                "Expected schema but header was " + result.getMessage.headerType());
            }

            schema = MessageSerializer.deserializeSchema(result.getMessage)
          }

          result = messageReader.readNext();
          if (result.getMessage.headerType() == MessageHeader.Schema) {
            result = messageReader.readNext();
          }

          if (result.getMessage.headerType() != MessageHeader.RecordBatch) {
            throw new IOException(
              "Expected recordbatch but header was " + result.getMessage.headerType());
          }
          var bodyBuffer = result.getBodyBuffer

          // For zero-length batches, need an empty buffer to deserialize the batch
          if (bodyBuffer == null) {
            bodyBuffer = allocator.getEmpty
          }

          val batch = MessageSerializer.deserializeRecordBatch(result.getMessage, bodyBuffer);
          val vectors = fromArrowRecordBatch(schema, batch, allocator)
          val length = batch.getLength
          batch.close()
          if (columnIndices == null) {
            new ColumnarBatch(vectors.map(_.asInstanceOf[ColumnVector]), length)
          } else {
            new ColumnarBatch(columnIndices.map(i => vectors(i).asInstanceOf[ColumnVector]), length)
          }

        } catch {
          case e: Throwable =>
            messageReader.close()
            throw e
        }
      }
    }
  }

  def fromArrowRecordBatch(
      recordBatchSchema: Schema,
      recordBatch: ArrowRecordBatch,
      allocator: BufferAllocator = null): Array[ArrowWritableColumnVector] = {
    val numRows = recordBatch.getLength()
    ArrowWritableColumnVector.loadColumns(numRows, recordBatchSchema, recordBatch, allocator)
  }

  private def getResultType(dataType: DataType): ArrowType = {
    getResultType(dataType, defaultTimeZoneId)
  }

  private def getResultType(dataType: DataType, timeZoneId: String): ArrowType = {
    dataType match {
      case other =>
        SparkArrowUtil.toArrowType(dataType, timeZoneId)
    }
  }

  def toArrowSchema(attributes: Seq[Attribute]): Schema = {
    val fields = attributes.map(
      attr => {
        Field
          .nullable(s"${attr.name}#${attr.exprId.id}", getResultType(attr.dataType))
      })
    new Schema(fields.toList.asJava)
  }

  def toArrowSchema(
      cSchema: ArrowSchema,
      allocator: BufferAllocator,
      provider: CDataDictionaryProvider): Schema = {
    val schema = Data.importSchema(allocator, cSchema, provider)
    val originFields = schema.getFields
    val fields = new util.ArrayList[Field](originFields.size)
    originFields.forEach {
      field =>
        val dt = SparkArrowUtil.fromArrowField(field)
        fields.add(
          SparkArrowUtil.toArrowField(field.getName, dt, true, SparkSchemaUtil.getLocalTimezoneID))
    }
    new Schema(fields)
  }

  def toSchema(batch: ColumnarBatch): Schema = {
    val fields = new java.util.ArrayList[Field](batch.numCols)
    for (i <- 0 until batch.numCols) {
      val col: ColumnVector = batch.column(i)
      fields.add(col match {
        case vector: ArrowWritableColumnVector =>
          vector.getValueVector.getField
        case _ =>
          throw new UnsupportedOperationException(
            s"Unexpected vector type: ${col.getClass.toString}")
      })
    }
    new Schema(fields)
  }
}
