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

package org.apache.spark.sql.execution

import java.io._

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import io.glutenproject.columnarbatch.ArrowColumnarBatches
import io.glutenproject.expression.ArrowConverterUtils
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.vectorized.{ArrowWritableColumnVector, SerializableObject}
import sun.misc.Cleaner

import org.apache.spark.sql.execution.ColumnarHashedRelation.Deallocator
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.KnownSizeEstimation

class ColumnarHashedRelation(
                              var hashRelationObj: SerializableObject,
                              var arrowColumnarBatch: Array[ColumnarBatch],
                              var arrowColumnarBatchSize: Int)
  extends Externalizable
    with KryoSerializable
    with KnownSizeEstimation {

  createCleaner(hashRelationObj, arrowColumnarBatch)

  def this() = {
    this(null, null, 0)
  }

  def asReadOnlyCopy(): ColumnarHashedRelation = {
    // new ColumnarHashedRelation(hashRelationObj, arrowColumnarBatch, arrowColumnarBatchSize)
    this
  }

  override def estimatedSize: Long = 0

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeObject(hashRelationObj)
    val rawArrowData = ArrowConverterUtils.convertToNetty(arrowColumnarBatch)
    out.writeObject(rawArrowData)
  }

  override def write(kryo: Kryo, out: Output): Unit = {
    kryo.writeObject(out, hashRelationObj)
    val rawArrowData = ArrowConverterUtils.convertToNetty(arrowColumnarBatch)
    kryo.writeObject(out, rawArrowData)
  }

  override def readExternal(in: ObjectInput): Unit = {
    hashRelationObj = in.readObject().asInstanceOf[SerializableObject]
    val rawArrowData = in.readObject().asInstanceOf[Array[Byte]]
    arrowColumnarBatchSize = rawArrowData.length
    arrowColumnarBatch =
      ArrowConverterUtils.convertFromNetty(null, new ByteArrayInputStream(rawArrowData)).toArray
    createCleaner(hashRelationObj, arrowColumnarBatch)
    // retain all cols
    /* arrowColumnarBatch.foreach(cb => {
      (0 until cb.numCols).toList.foreach(i =>
        cb.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
    }) */
  }

  private def createCleaner(obj: SerializableObject, batch: Array[ColumnarBatch]): Unit = {
    if (obj == null && batch == null) {
      // no need to clean up
      return
    }
    Cleaner.create(this, new Deallocator(obj, batch))
  }

  override def read(kryo: Kryo, in: Input): Unit = {
    hashRelationObj =
      kryo.readObject(in, classOf[SerializableObject]).asInstanceOf[SerializableObject]
    val rawArrowData = kryo.readObject(in, classOf[Array[Byte]]).asInstanceOf[Array[Byte]]
    arrowColumnarBatchSize = rawArrowData.length
    arrowColumnarBatch =
      ArrowConverterUtils.convertFromNetty(null, new ByteArrayInputStream(rawArrowData)).toArray
    createCleaner(hashRelationObj, arrowColumnarBatch)
    // retain all cols
    /* arrowColumnarBatch.foreach(cb => {
      (0 until cb.numCols).toList.foreach(i =>
        cb.column(i).asInstanceOf[ArrowWr:w
        itableColumnVector].retain())
    }) */
  }

  def size(): Int = {
    hashRelationObj.total_size + arrowColumnarBatchSize
  }

  def getColumnarBatchAsIter: Iterator[ColumnarBatch] = {
    new Iterator[ColumnarBatch] {
      var idx = 0
      val total_len = arrowColumnarBatch.length

      override def hasNext: Boolean = idx < total_len

      override def next(): ColumnarBatch = {
        val tmp_idx = idx
        idx += 1
        val cb = arrowColumnarBatch(tmp_idx)
        // retain all cols
        (0 until cb.numCols).toList.foreach(i =>
          ArrowColumnarBatches
            .ensureLoaded(ArrowBufferAllocators.contextInstance(),
              cb).column(i).asInstanceOf[ArrowWritableColumnVector].retain())
        cb
      }
    }
  }
}

object ColumnarHashedRelation {

  private class Deallocator(
                             var hashRelationObj: SerializableObject,
                             var arrowColumnarBatch: Array[ColumnarBatch]) extends Runnable {

    override def run(): Unit = {
      try {
        Option(hashRelationObj).foreach(_.close())
        Option(arrowColumnarBatch).foreach(_.foreach(_.close))
      } catch {
        case e: Exception =>
          // We should suppress all possible errors in Cleaner to prevent JVM from being shut down
          // scalastyle:off println
          System.err.println("ColumnarHashedRelation: Error running deaallocator")
          // scalastyle:on println
          e.printStackTrace()
      }
    }
  }
}
