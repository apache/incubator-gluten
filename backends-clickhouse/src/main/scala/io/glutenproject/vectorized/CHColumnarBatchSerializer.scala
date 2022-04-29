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

import java.io.{EOFException, InputStream, OutputStream, Serializable}
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.SparkEnv

import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch

class CHColumnarBatchSerializer(readBatchNumRows: SQLMetric, numOutputRows: SQLMetric)
  extends Serializer with Serializable {
  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance = {
    new CHColumnarBatchSerializerInstance(readBatchNumRows, numOutputRows)
  }
}


private class CHColumnarBatchSerializerInstance(readBatchNumRows: SQLMetric,
                                                numOutputRows: SQLMetric)
  extends SerializerInstance
    with Logging {

  override def deserializeStream(in: InputStream): DeserializationStream = {
    new DeserializationStream {

      private val compressionEnabled =
        SparkEnv.get.conf.getBoolean("spark.shuffle.compress", true)

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

          val nativeBlock = reader.next()
          if (nativeBlock.numRows() > 0) {
            val numRows = nativeBlock.numRows()
            logDebug(s"Read ColumnarBatch of ${numRows} rows")

            numBatchesTotal += 1
            numRowsTotal += numRows
            cb = nativeBlock.toColumnarBatch
            cb.asInstanceOf[T]
          } else {
            this.close()
            throw new EOFException
          }
        } else {
          reader = new CHStreamReader(in)
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
          if (cb != null) cb = null
          if (reader != null) reader.close()
          isClosed = true
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