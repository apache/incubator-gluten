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

package org.apache.spark.sql.execution.utils

import scala.collection.JavaConverters.asScalaIteratorConverter

import io.glutenproject.columnarbatch.{ArrowColumnarBatches, GlutenColumnarBatches}
import io.glutenproject.memory.alloc.NativeMemoryAllocators
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.vectorized.{ArrowWritableColumnVector, NativeColumnarToRowInfo, NativeColumnarToRowJniWrapper, NativePartitioning}

import org.apache.spark.{Partitioner, RangePartitioner, ShuffleDependency}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.ColumnarShuffleDependency
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.PartitionIdPassthrough
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.MutablePair
import org.apache.spark.util.memory.TaskResources

object GlutenExecUtil {

  def convertColumnarToRow(batch: ColumnarBatch): Iterator[InternalRow] = {
    val jniWrapper = new NativeColumnarToRowJniWrapper()
    var info: NativeColumnarToRowInfo = null
    val offloaded =
      ArrowColumnarBatches.ensureOffloaded(ArrowBufferAllocators.contextInstance(), batch)
    val batchHandle = GlutenColumnarBatches.getNativeHandle(offloaded)
    info = jniWrapper.nativeConvertColumnarToRow(
      batchHandle,
      NativeMemoryAllocators.contextInstance().getNativeInstanceId)


    new Iterator[InternalRow] {
      var rowId = 0
      val row = new UnsafeRow(batch.numCols())
      var closed = false

      TaskResources.addRecycler(100) {
        if (!closed) {
          jniWrapper.nativeClose(info.instanceID)
          closed = true
        }
      }

      override def hasNext: Boolean = {
        val result = rowId < batch.numRows()
        if (!result && !closed) {
          jniWrapper.nativeClose(info.instanceID)
          closed = true
        }
        result
      }

      override def next: UnsafeRow = {
        if (rowId >= batch.numRows()) throw new NoSuchElementException
        val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
        row.pointTo(null, info.memoryAddress + offset, length.toInt)
        rowId += 1
        row
      }
    }
  }

  // scalastyle:off argcount
  def genShuffleDependency(rdd: RDD[ColumnarBatch],
                           outputAttributes: Seq[Attribute],
                           newPartitioning: Partitioning,
                           serializer: Serializer,
                           writeMetrics: Map[String, SQLMetric],
                           metrics: Map[String, SQLMetric]
                          ): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    // scalastyle:on argcount
    // only used for fallback range partitioning
    val rangePartitioner: Option[Partitioner] = newPartitioning match {
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Extract only fields used for sorting to avoid collecting large fields that does not
        // affect sorting result when deciding partition bounds in RangePartitioner
        val rddForSampling = rdd.mapPartitionsInternal { iter =>
          // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
          // partition bounds. To get accurate samples, we need to copy the mutable keys.
          iter.flatMap(batch => {
            val rows = if (GlutenColumnarBatches.isIntermediateColumnarBatch(batch)) {
              convertColumnarToRow(batch)
            } else ArrowColumnarBatches
              .ensureLoaded(ArrowBufferAllocators.contextInstance(), batch)
              .rowIterator.asScala
            val projection =
              UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
            val mutablePair = new MutablePair[InternalRow, Null]()
            rows.map(row => mutablePair.update(projection(row).copy(), null))
          })
        }
        // Construct ordering on extracted sort key.
        val orderingAttributes = sortingExpressions.zipWithIndex.map {
          case (ord, i) =>
            ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
        }
        implicit val ordering = new LazilyGeneratedOrdering(orderingAttributes)
        val part = new RangePartitioner(
          numPartitions,
          rddForSampling,
          ascending = true,
          samplePointsPerPartitionHint = SQLConf.get.rangeExchangeSampleSizePerPartition)
        Some(part)
      case _ => None
    }

    // will remove after arrow is removed
    def computeArrowAddPartitionId(cb: ColumnarBatch, partitionKeyExtractor: InternalRow => Any):
    (Int, ColumnarBatch) = {
      val pidVec = ArrowWritableColumnVector
        .allocateColumns(cb.numRows, new StructType().add("pid", IntegerType))
        .head
      (0 until cb.numRows).foreach { i =>
        val row = cb.getRow(i)
        val pid = rangePartitioner.get.getPartition(partitionKeyExtractor(row))
        pidVec.putInt(i, pid)
      }

      val newColumns = (pidVec +: (0 until cb.numCols).map(
        ArrowColumnarBatches
          .ensureLoaded(ArrowBufferAllocators.contextInstance(),
            cb).column)).toArray
      val offloadCb = ArrowColumnarBatches.ensureOffloaded(
        ArrowBufferAllocators.contextInstance(),
        new ColumnarBatch(newColumns, cb.numRows))
      (0, offloadCb)
    }

    // only used for fallback range partitioning
    def computeAndAddPartitionId(cbIter: Iterator[ColumnarBatch],
                                 partitionKeyExtractor: InternalRow => Any
                                ): CloseablePairedColumnarBatchIterator = {
      var batchType: String = null
      CloseablePairedColumnarBatchIterator {
        cbIter
          .filter(cb => cb.numRows != 0 && cb.numCols != 0)
          .map { cb => if (GlutenColumnarBatches.isIntermediateColumnarBatch(cb)) {
            if (batchType == null) {
              batchType = ArrowColumnarBatches.getType(cb)
            }
            if (batchType.equals("velox")) {
              val pidVec = ArrowWritableColumnVector
                .allocateColumns(cb.numRows, new StructType().add("pid", IntegerType))
                .head
              convertColumnarToRow(cb).zipWithIndex.foreach {
                case (row, i) =>
                  val pid = rangePartitioner.get.getPartition(partitionKeyExtractor(row))
                  pidVec.putInt(i, pid)
              }
              val pidBatch = new ColumnarBatch(Array[ColumnVector](pidVec), cb.numRows)
              val offloadColCb = ArrowColumnarBatches.ensureOffloaded(
                ArrowBufferAllocators.contextInstance(),
                pidBatch)
              val newHandle = ArrowColumnarBatches.addColumn(cb, 0, offloadColCb)
              (0, GlutenColumnarBatches.create(newHandle))
            } else {
              computeArrowAddPartitionId(cb, partitionKeyExtractor)
            }
          } else {
            val loaded = ArrowColumnarBatches.ensureLoaded(
              ArrowBufferAllocators.contextInstance(), cb)
            computeArrowAddPartitionId(loaded, partitionKeyExtractor)
          }
          }
      }
    }

    val nativePartitioning: NativePartitioning = newPartitioning match {
      case SinglePartition =>
        new NativePartitioning("single", 1)
      case RoundRobinPartitioning(n) =>
        new NativePartitioning("rr", n)
      case HashPartitioning(exprs, n) =>
        new NativePartitioning("hash", n)
      // range partitioning fall back to row-based partition id computation
      case RangePartitioning(orders, n) =>
        new NativePartitioning("range", n)
    }

    val isRoundRobin = newPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      newPartitioning.numPartitions > 1

    // RDD passed to ShuffleDependency should be the form of key-value pairs.
    // ColumnarShuffleWriter will compute ids from ColumnarBatch on native side
    // other than read the "key" part.
    // Thus in Columnar Shuffle we never use the "key" part.
    val isOrderSensitive = isRoundRobin && !SQLConf.get.sortBeforeRepartition

    val rddWithDummyKey: RDD[Product2[Int, ColumnarBatch]] = newPartitioning match {
      case RangePartitioning(sortingExpressions, _) =>
        rdd.mapPartitionsWithIndexInternal((_, cbIter) => {
          val partitionKeyExtractor: InternalRow => Any = {
            val projection =
              UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
            row => projection(row)
          }
          val newIter = computeAndAddPartitionId(cbIter, partitionKeyExtractor)

          TaskResources.addRecycler(100) {
            newIter.closeColumnBatch()
          }

          newIter
        }, isOrderSensitive = isOrderSensitive)
      case _ =>
        rdd.mapPartitionsWithIndexInternal(
          (_, cbIter) =>
            cbIter.map { cb =>
              (0, ArrowColumnarBatches.ensureOffloaded(
                ArrowBufferAllocators.contextInstance(),
                cb))
            },
          isOrderSensitive = isOrderSensitive)
    }

    val dependency =
      new ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
        rddWithDummyKey,
        new PartitionIdPassthrough(newPartitioning.numPartitions),
        serializer,
        shuffleWriterProcessor =
          ShuffleExchangeExec.createShuffleWriteProcessor(writeMetrics),
        nativePartitioning = nativePartitioning,
        metrics = metrics)

    dependency
  }
}


case class CloseablePairedColumnarBatchIterator(iter: Iterator[(Int, ColumnarBatch)])
  extends Iterator[(Int, ColumnarBatch)]
    with Logging {

  private var cur: (Int, ColumnarBatch) = _

  override def hasNext: Boolean = {
    iter.hasNext
  }

  override def next(): (Int, ColumnarBatch) = {
    closeColumnBatch()
    if (iter.hasNext) {
      cur = iter.next()
      cur
    } else Iterator.empty.next()
  }

  def closeColumnBatch(): Unit = {
    if (cur != null) {
      logDebug("Close appended partition id vector")
      cur match {
        case (_, cb: ColumnarBatch) => ArrowColumnarBatches.close(cb)
      }
      cur = null
    }
  }
}
