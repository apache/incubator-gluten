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

import io.glutenproject.columnarbatch.ColumnarBatches
import io.glutenproject.exec.ExecutionCtxs
import io.glutenproject.memory.arrowalloc.ArrowBufferAllocators
import io.glutenproject.memory.nmm.NativeMemoryManagers
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
import org.apache.spark.util.{MutablePair, TaskResources}

object ExecUtil {

  def convertColumnarToRow(batch: ColumnarBatch): Iterator[InternalRow] = {
    val executionCtxHandle = ExecutionCtxs.contextInstance().getHandle
    val jniWrapper = new NativeColumnarToRowJniWrapper()
    var info: NativeColumnarToRowInfo = null
    val batchHandle = ColumnarBatches.getNativeHandle(batch)
    val c2rHandle = jniWrapper.nativeColumnarToRowInit(
      executionCtxHandle,
      NativeMemoryManagers
        .contextInstance("ExecUtil#ColumnarToRow")
        .getNativeInstanceHandle)
    info = jniWrapper.nativeColumnarToRowConvert(executionCtxHandle, batchHandle, c2rHandle)

    new Iterator[InternalRow] {
      var rowId = 0
      val row = new UnsafeRow(batch.numCols())
      var closed = false

      TaskResources.addRecycler(s"ColumnarToRow_$c2rHandle", 100) {
        if (!closed) {
          jniWrapper.nativeClose(executionCtxHandle, c2rHandle)
          closed = true
        }
      }

      override def hasNext: Boolean = {
        val result = rowId < batch.numRows()
        if (!result && !closed) {
          jniWrapper.nativeClose(executionCtxHandle, c2rHandle)
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
  def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      metrics: Map[String, SQLMetric]): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    // scalastyle:on argcount
    // only used for fallback range partitioning
    val rangePartitioner: Option[Partitioner] = newPartitioning match {
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Extract only fields used for sorting to avoid collecting large fields that does not
        // affect sorting result when deciding partition bounds in RangePartitioner
        val rddForSampling = rdd.mapPartitionsInternal {
          iter =>
            // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
            // partition bounds. To get accurate samples, we need to copy the mutable keys.
            iter.flatMap(
              batch => {
                val rows = convertColumnarToRow(batch)
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

    // only used for fallback range partitioning
    def computeAndAddPartitionId(
        cbIter: Iterator[ColumnarBatch],
        partitionKeyExtractor: InternalRow => Any): CloseablePairedColumnarBatchIterator = {
      CloseablePairedColumnarBatchIterator {
        cbIter
          .filter(cb => cb.numRows != 0 && cb.numCols != 0)
          .map {
            cb =>
              val pidVec = ArrowWritableColumnVector
                .allocateColumns(cb.numRows, new StructType().add("pid", IntegerType))
                .head
              convertColumnarToRow(cb).zipWithIndex.foreach {
                case (row, i) =>
                  val pid = rangePartitioner.get.getPartition(partitionKeyExtractor(row))
                  pidVec.putInt(i, pid)
              }
              val pidBatch = ColumnarBatches.ensureOffloaded(
                ArrowBufferAllocators.contextInstance(),
                new ColumnarBatch(Array[ColumnVector](pidVec), cb.numRows))
              val newHandle = ColumnarBatches.compose(pidBatch, cb)
              (0, ColumnarBatches.create(ColumnarBatches.getExecutionCtxHandle(cb), newHandle))
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
        rdd.mapPartitionsWithIndexInternal(
          (_, cbIter) => {
            val partitionKeyExtractor: InternalRow => Any = {
              val projection =
                UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
              row => projection(row)
            }
            val newIter = computeAndAddPartitionId(cbIter, partitionKeyExtractor)

            TaskResources.addRecycler("RangePartitioningIter", 100) {
              newIter.closeColumnBatch()
            }

            newIter
          },
          isOrderSensitive = isOrderSensitive
        )
      case _ =>
        rdd.mapPartitionsWithIndexInternal(
          (_, cbIter) => cbIter.map(cb => (0, cb)),
          isOrderSensitive = isOrderSensitive)
    }

    val dependency =
      new ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
        rddWithDummyKey,
        new PartitionIdPassthrough(newPartitioning.numPartitions),
        serializer,
        shuffleWriterProcessor = ShuffleExchangeExec.createShuffleWriteProcessor(writeMetrics),
        nativePartitioning = nativePartitioning,
        metrics = metrics
      )

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
        case (_, cb: ColumnarBatch) => ColumnarBatches.close(cb)
      }
      cur = null
    }
  }
}
