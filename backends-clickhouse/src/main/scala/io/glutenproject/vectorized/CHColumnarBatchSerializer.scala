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

import io.glutenproject.GlutenConfig
import io.glutenproject.backendsapi.clickhouse.CHBackendSettings

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

import java.io._
import java.nio.ByteBuffer

import scala.reflect.ClassTag

class CHColumnarBatchSerializer(
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric,
    dataSize: SQLMetric)
  extends Serializer
  with Serializable {

  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance = {
    new CHColumnarBatchSerializerInstance(readBatchNumRows, numOutputRows, dataSize)
  }

  override def supportsRelocationOfSerializedObjects: Boolean = true
}

private class CHColumnarBatchSerializerInstance(
    readBatchNumRows: SQLMetric,
    numOutputRows: SQLMetric,
    dataSize: SQLMetric)
  extends SerializerInstance
  with Logging {

  private lazy val isUseColumnarShufflemanager =
    GlutenConfig.getConf.isUseColumnarShuffleManager
  private lazy val customizeBufferSize = SparkEnv.get.conf.getInt(
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE,
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_BUFFER_SIZE_DEFAULT.toInt
  )
  private lazy val isCustomizedShuffleCodec = SparkEnv.get.conf.getBoolean(
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE,
    CHBackendSettings.GLUTEN_CLICKHOUSE_CUSTOMIZED_SHUFFLE_CODEC_ENABLE_DEFAULT.toBoolean
  )
  private lazy val compressionCodec =
    GlutenConfig.getConf.columnarShuffleUseCustomizedCompressionCodec

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {

      private var reader: CHStreamReader = _
      private var cb: ColumnarBatch = _

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
        if (reader != null) {
          if (cb != null) {
            cb.close()
            cb = null
          }

          var nativeBlock = reader.next()
          while (nativeBlock.numRows() == 0) {
            if (nativeBlock.numColumns() == 0) {
              nativeBlock.close()
              this.close()
              throw new EOFException
            }
            nativeBlock = reader.next()
          }
          val numRows = nativeBlock.numRows()

          numBatchesTotal += 1
          numRowsTotal += numRows
          cb = nativeBlock.toColumnarBatch
          cb.asInstanceOf[T]
        } else {
          reader = new CHStreamReader(
            in,
            isUseColumnarShufflemanager,
            isCustomizedShuffleCodec,
            customizeBufferSize)
          readValue()
        }
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
          if (cb != null) {
            cb.close()
            cb = null
          }
          if (reader != null) reader.close()
          isClosed = true
        }
      }
    }
  }

  override def serializeStream(out: OutputStream): SerializationStream = new SerializationStream {
    private[this] var writeBuffer: Array[Byte] = new Array[Byte](customizeBufferSize)
    private[this] var dOut: BlockOutputStream =
      new BlockOutputStream(
        out,
        writeBuffer,
        dataSize,
        isCustomizedShuffleCodec,
        compressionCodec,
        customizeBufferSize
      )

    override def writeKey[T: ClassTag](key: T): SerializationStream = {
      // The key is only needed on the map side when computing partition ids. It does not need to
      // be shuffled.
      assert(null == key || key.isInstanceOf[Int])
      this
    }

    override def writeValue[T: ClassTag](value: T): SerializationStream = {
      val cb = value.asInstanceOf[ColumnarBatch]

      // Use for reading bytes array from block
      dOut.write(cb)
      this
    }

    override def writeAll[T: ClassTag](iter: Iterator[T]): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def writeObject[T: ClassTag](t: T): SerializationStream = {
      // This method is never called by shuffle code.
      throw new UnsupportedOperationException
    }

    override def flush(): Unit = {
      dOut.flush()
    }

    override def close(): Unit = {
      dOut.close()
      writeBuffer = null
    }
  }

  // These methods are never called by shuffle code.
  override def serialize[T: ClassTag](t: T): ByteBuffer = throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T =
    throw new UnsupportedOperationException

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T =
    throw new UnsupportedOperationException

}
