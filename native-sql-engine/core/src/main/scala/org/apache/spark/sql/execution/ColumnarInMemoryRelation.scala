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
import org.apache.commons.lang3.StringUtils

import com.intel.oap.expression._
import com.intel.oap.vectorized.ArrowWritableColumnVector
import com.intel.oap.vectorized.CloseableColumnBatchIterator
import org.apache.arrow.memory.ArrowBuf
import org.apache.spark.TaskContext
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{logical, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.columnar.{
  CachedBatch,
  CachedBatchSerializer,
  SimpleMetricsCachedBatch,
  SimpleMetricsCachedBatchSerializer
}
import org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.vectorized.{WritableColumnVector}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{LongAccumulator, Utils, KnownSizeEstimation}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

/**
 * The default implementation of CachedBatch.
 *
 * @param numRows The total number of rows in this batch
 * @param buffers The buffers for serialized columns
 * @param stats The stat of columns
 */
case class ArrowCachedBatch(
    var numRows: Int,
    var buffer: Array[ColumnarBatch],
    stats: InternalRow)
    extends SimpleMetricsCachedBatch
    with KryoSerializable
    with Externalizable 
    with KnownSizeEstimation
    with AutoCloseable {
  def this() = {
    this(0, null, null)
  }
  override def close() = {
    buffer.foreach(_.close)
  }
  override def estimatedSize: Long = {
    var size: Long = 0
    buffer.foreach(batch => {
      size += ConverterUtils.calcuateEstimatedSize(batch)
    })
    size
  }
  override def sizeInBytes: Long = estimatedSize
  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeObject(numRows)
    val rawArrowData = ConverterUtils.convertToNetty(buffer)
    out.writeObject(rawArrowData)
    buffer.foreach(_.close)
  }
  override def write(kryo: Kryo, out: Output): Unit = {
    kryo.writeObject(out, numRows)
    val rawArrowData = ConverterUtils.convertToNetty(buffer)
    kryo.writeObject(out, rawArrowData)
    System.out.println("ArrowCachedBatch close when write to disk")
    buffer.foreach(_.close)
  }
  override def readExternal(in: ObjectInput): Unit = {
    numRows = in.readObject().asInstanceOf[Integer]
    val rawArrowData = in.readObject().asInstanceOf[Array[Byte]]
    buffer = ConverterUtils.convertFromNetty(null, new ByteArrayInputStream(rawArrowData)).toArray
  }
  override def read(kryo: Kryo, in: Input): Unit = {
    numRows = kryo.readObject(in, classOf[Integer]).asInstanceOf[Integer]
    val rawArrowData = kryo.readObject(in, classOf[Array[Byte]]).asInstanceOf[Array[Byte]]
    buffer = ConverterUtils.convertFromNetty(null, new ByteArrayInputStream(rawArrowData)).toArray
  }
}

/**
 * The default implementation of CachedBatchSerializer.
 */
class ArrowColumnarCachedBatchSerializer extends DefaultCachedBatchSerializer {
  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = true
  override def supportsColumnarOutput(schema: StructType): Boolean =
    isInputSupportColumnar || (!isInputSupportColumnar && super.supportsColumnarOutput(schema))
  var isInputSupportColumnar: Boolean = true

  override def vectorTypes(attributes: Seq[Attribute], conf: SQLConf): Option[Seq[String]] =
    Option(Seq.fill(attributes.length)(classOf[ArrowWritableColumnVector].getName))

  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    isInputSupportColumnar = true
    val batchSize = conf.columnBatchSize
    val useCompression = conf.useCompression
    convertForColumnarCacheInternal(input, schema, batchSize, useCompression)
  }

  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    isInputSupportColumnar = false
    super.convertInternalRowToCachedBatch(input, schema, storageLevel, conf)
  }

  def convertForColumnarCacheInternal(
      input: RDD[ColumnarBatch],
      output: Seq[Attribute],
      batchSize: Int,
      useCompression: Boolean): RDD[CachedBatch] = {
    input.mapPartitions { iter =>
      var processed = false
      new Iterator[ArrowCachedBatch] {
        def next(): ArrowCachedBatch = {
          processed = true
          var _numRows: Int = 0
          val _input = new ArrayBuffer[ColumnarBatch]()
          while (iter.hasNext) {
            val batch = iter.next
            if (batch.numRows > 0) {
              (0 until batch.numCols).foreach(i =>
                batch.column(i).asInstanceOf[ArrowWritableColumnVector].retain())
              _numRows += batch.numRows
              _input += batch
            }
          }
          // To avoid mem copy, we only save columnVector reference here
          val res = ArrowCachedBatch(_numRows, _input.toArray, null)
          res
        }

        def hasNext: Boolean = !processed
      }
    }
  }

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    if (isInputSupportColumnar) {
      val columnIndices =
        selectedAttributes.map(a => cacheAttributes.map(o => o.exprId).indexOf(a.exprId)).toArray
      def createAndDecompressColumn(cachedIter: Iterator[CachedBatch]): Iterator[ColumnarBatch] = {
        val res = new Iterator[ColumnarBatch] {
          var iter: Iterator[ColumnarBatch] = null
          if (cachedIter.hasNext) {
            val cachedColumnarBatch: ArrowCachedBatch =
              cachedIter.next.asInstanceOf[ArrowCachedBatch]
            val rawData = cachedColumnarBatch.buffer

            iter = new Iterator[ColumnarBatch] {
              val numBatches = rawData.size
              var batchIdx = 0
              override def hasNext: Boolean = batchIdx < numBatches
              override def next(): ColumnarBatch = {
                val vectors = columnIndices.map(i => rawData(batchIdx).column(i))
                vectors.foreach(v => v.asInstanceOf[ArrowWritableColumnVector].retain())
                val numRows = rawData(batchIdx).numRows
                batchIdx += 1
                new ColumnarBatch(vectors, numRows)
              }
            }
          }
          def next(): ColumnarBatch =
            if (iter != null) {
              iter.next
            } else {
              val resultStructType = StructType(selectedAttributes.map(a =>
                StructField(a.name, a.dataType, a.nullable, a.metadata)))
              val resultColumnVectors =
                ArrowWritableColumnVector.allocateColumns(0, resultStructType).toArray
              new ColumnarBatch(resultColumnVectors.map(_.asInstanceOf[ColumnVector]), 0)
            }
          def hasNext: Boolean = iter.hasNext
        }
        new CloseableColumnBatchIterator(res)
      }
      input.mapPartitions(createAndDecompressColumn)
    } else {
      super.convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
    }
  }

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    if (isInputSupportColumnar) {
      // Find the ordinals and data types of the requested columns.
      val columnarBatchRdd =
        convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
      columnarBatchRdd.mapPartitions { batches =>
        val toUnsafe = UnsafeProjection.create(selectedAttributes, selectedAttributes)
        batches.flatMap { batch => batch.rowIterator().asScala.map(toUnsafe) }
      }
    } else {
      super.convertCachedBatchToInternalRow(input, cacheAttributes, selectedAttributes, conf)
    }
  }

}
