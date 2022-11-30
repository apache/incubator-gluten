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

package io.glutenproject.expression

import java.io._
import java.nio.channels.Channels
import scala.collection.JavaConverters._
import com.google.common.collect.Lists
import io.glutenproject.columnarbatch.ArrowColumnarBatches
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.vectorized.ArrowWritableColumnVector
import io.netty.buffer.{ByteBufAllocator, ByteBufOutputStream}
import org.apache.arrow.c.{ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.flatbuf.MessageHeader
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{FieldVector, ValueVector}
import org.apache.arrow.vector.ipc.{ReadChannel, WriteChannel}
import org.apache.arrow.vector.ipc.message._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.v2.arrow.{SparkSchemaUtils, SparkVectorUtils}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

import java.util

object ArrowConverterUtils extends Logging {

  def calcuateEstimatedSize(columnarBatch: ColumnarBatch): Long = {
    SparkVectorUtils.estimateSize(columnarBatch)
  }

  def createArrowRecordBatch(columnarBatch: ColumnarBatch): ArrowRecordBatch = {
    SparkVectorUtils.toArrowRecordBatch(columnarBatch)
  }

  def createFieldVectorList(columnarBatch: ColumnarBatch): List[FieldVector] = {
    SparkVectorUtils.toFieldVectorList(columnarBatch)
  }

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

    iter.foreach { columnarBatch =>
      val vectors = (0 until columnarBatch.numCols)
        .map(i => ArrowColumnarBatches
          .ensureLoaded(ArrowBufferAllocators.contextInstance(), columnarBatch)
          .column(i).asInstanceOf[ArrowWritableColumnVector])
        .toList
      try {
        if (schema == null) {
          schema = new Schema(vectors.map(_.getValueVector().getField).asJava)
          MessageSerializer.serialize(channel, schema, option)
        }
        val batch = ArrowConverterUtils
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
    SparkVectorUtils.toArrowRecordBatch(numRowsInBatch, cols)
  }

  def convertFromNetty(attributes: Seq[Attribute],
                       input: InputStream): Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      val allocator = ArrowBufferAllocators.contextInstance()
      var messageReader =
        new MessageChannelReader(new ReadChannel(Channels.newChannel(input)), allocator)
      var schema: Schema = null
      var result: MessageResult = null

      override def hasNext: Boolean =
        if (input.available > 0) {
          return true
        } else {
          messageReader.close
          return false
        }

      override def next(): ColumnarBatch = {
        if (input.available == 0) {
          if (attributes == null) {
            return null
          }
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
            if (result.getMessage().headerType() != MessageHeader.Schema) {
              throw new IOException(
                "Expected schema but header was " + result.getMessage().headerType());
            }
            schema = MessageSerializer.deserializeSchema(result.getMessage());
          }
          result = messageReader.readNext();
          if (result.getMessage().headerType() != MessageHeader.RecordBatch) {
            throw new IOException(
              "Expected recordbatch but header was " + result.getMessage().headerType());
          }
          var bodyBuffer = result.getBodyBuffer();

          // For zero-length batches, need an empty buffer to deserialize the batch
          if (bodyBuffer == null) {
            bodyBuffer = allocator.getEmpty();
          }

          val batch = MessageSerializer.deserializeRecordBatch(result.getMessage(), bodyBuffer);
          val vectors = fromArrowRecordBatch(schema, batch, allocator)
          val length = batch.getLength
          batch.close
          new ColumnarBatch(vectors.map(_.asInstanceOf[ColumnVector]), length)
        } catch {
          case e: Throwable =>
            messageReader.close
            throw e
        }
      }
    }
  }

  def convertFromNetty(attributes: Seq[Attribute],
                       data: Array[Array[Byte]],
                       columnIndices: Array[Int] = null): Iterator[ColumnarBatch] = {
    if (data.length == 0) {
      return new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = false

        override def next(): ColumnarBatch = {
          val resultStructType = if (columnIndices == null) {
            StructType(
              attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
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
            new ColumnarBatch(
              columnIndices.map(i => vectors(i).asInstanceOf[ColumnVector]),
              length)
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

  def releaseArrowRecordBatchList(recordBatchList: Array[ArrowRecordBatch]): Unit = {
    recordBatchList.foreach({ recordBatch =>
      if (recordBatch != null) {
        releaseArrowRecordBatch(recordBatch)
      }
    })
  }

  def releaseArrowRecordBatch(recordBatch: ArrowRecordBatch): Unit = {
    if (recordBatch != null) {
      recordBatch.close()
    }
  }

  def combineArrowRecordBatch(rb1: ArrowRecordBatch, rb2: ArrowRecordBatch): ArrowRecordBatch = {
    val numRows = rb1.getLength()
    val rb1_nodes = rb1.getNodes()
    val rb2_nodes = rb2.getNodes()
    val rb1_bufferlist = rb1.getBuffers()
    val rb2_bufferlist = rb2.getBuffers()

    val combined_nodes = rb1_nodes.addAll(rb2_nodes)
    val combined_bufferlist = rb1_bufferlist.addAll(rb2_bufferlist)
    new ArrowRecordBatch(numRows, rb1_nodes, rb1_bufferlist)
  }

  def toArrowSchema(attributes: Seq[Attribute]): Schema = {
    val fields = attributes.map(attr => {
      Field
        .nullable(s"${attr.name}#${attr.exprId.id}", CodeGeneration.getResultType(attr.dataType))
    })
    new Schema(fields.toList.asJava)
  }

  def toArrowSchema(schema: StructType): Schema = {
    val fields = schema
      .map(field => {
        Field.nullable(field.name, CodeGeneration.getResultType(field.dataType))
      })
    new Schema(fields.toList.asJava)
  }

  def toArrowSchema(cSchema: ArrowSchema,
                    allocator: BufferAllocator,
                    provider: CDataDictionaryProvider): Schema = {
    val schema = Data.importSchema(allocator, cSchema, provider)
    val originFields = schema.getFields
    val fields = new util.ArrayList[Field](originFields.size)
    originFields.forEach { field =>
      val dt = ArrowUtils.fromArrowField(field)
      fields.add(ArrowUtils.toArrowField("col",
        dt, true, SparkSchemaUtils.getLocalTimezoneID))
    }
    new Schema(fields)
  }


  @throws[IOException]
  def getSchemaBytesBuf(schema: Schema): Array[Byte] = {
    val out: ByteArrayOutputStream = new ByteArrayOutputStream
    MessageSerializer.serialize(new WriteChannel(Channels.newChannel(out)), schema)
    out.toByteArray
  }

  @throws[IOException]
  def getSchemaFromBytesBuf(schema: Array[Byte]): Schema = {
    val in: ByteArrayInputStream = new ByteArrayInputStream(schema)
    MessageSerializer.deserializeSchema(new ReadChannel(Channels.newChannel(in)))
  }

  def checkIfTypeSupported(dt: DataType): Unit = dt match {
    case d: BooleanType =>
    case d: ByteType =>
    case d: ShortType =>
    case d: IntegerType =>
    case d: LongType =>
    case d: FloatType =>
    case d: DoubleType =>
    case d: StringType =>
    case d: DateType =>
    case d: DecimalType =>
    case d: TimestampType =>
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported data type: $dt")
  }

  def createArrowField(name: String, dt: DataType): Field = dt match {
    case at: ArrayType =>
      new Field(
        name,
        FieldType.nullable(ArrowType.List.INSTANCE),
        Lists.newArrayList(createArrowField(s"${name}_${dt}", at.elementType)))
    case mt: MapType =>
      throw new UnsupportedOperationException(s"${dt} is not supported yet")
    case st: StructType =>
      throw new UnsupportedOperationException(s"${dt} is not supported yet")
    case _ =>
      Field.nullable(name, CodeGeneration.getResultType(dt))
  }

  def createArrowField(attr: Attribute): Field =
    createArrowField(s"${attr.name}#${attr.exprId.id}", attr.dataType)

  private def asTimestampType(inType: ArrowType): ArrowType.Timestamp = {
    if (inType.getTypeID != ArrowTypeID.Timestamp) {
      throw new IllegalArgumentException(s"Value type to convert must be timestamp")
    }
    inType.asInstanceOf[ArrowType.Timestamp]
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

  override def toString: String = {
    s"ArrowConverterUtils"
  }
}
