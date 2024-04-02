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

import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.memory.arrowalloc.ArrowBufferAllocators
import org.apache.gluten.memory.nmm.NativeMemoryManagers
import org.apache.gluten.utils.Iterators
import org.apache.gluten.vectorized.{ArrowWritableColumnVector, NativeColumnarToRowInfo, NativeColumnarToRowJniWrapper, NativePartitioning}

import org.apache.spark.{Partitioner, RangePartitioner, ShuffleDependency}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{ColumnarShuffleDependency, GlutenShuffleUtils}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.util.MutablePair

object ExecUtil {

  def convertColumnarToRow(batch: ColumnarBatch): Iterator[InternalRow] = {
    val jniWrapper = NativeColumnarToRowJniWrapper.create()
    var info: NativeColumnarToRowInfo = null
    val batchHandle = ColumnarBatches.getNativeHandle(batch)
    val c2rHandle = jniWrapper.nativeColumnarToRowInit(
      NativeMemoryManagers
        .contextInstance("ExecUtil#ColumnarToRow")
        .getNativeInstanceHandle)
    info = jniWrapper.nativeColumnarToRowConvert(batchHandle, c2rHandle)

    Iterators
      .wrap(new Iterator[InternalRow] {
        var rowId = 0
        val row = new UnsafeRow(batch.numCols())

        override def hasNext: Boolean = {
          rowId < batch.numRows()
        }

        override def next: UnsafeRow = {
          if (rowId >= batch.numRows()) throw new NoSuchElementException
          val (offset, length) = (info.offsets(rowId), info.lengths(rowId))
          row.pointTo(null, info.memoryAddress + offset, length.toInt)
          rowId += 1
          row
        }
      })
      .recycleIterator {
        jniWrapper.nativeClose(c2rHandle)
      }
      .create()
  }

  // scalastyle:off argcount
  def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      metrics: Map[String, SQLMetric]): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {
    metrics("numPartitions").set(newPartitioning.numPartitions)
    val executionId = rdd.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      rdd.sparkContext,
      executionId,
      metrics("numPartitions") :: Nil)
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
        partitionKeyExtractor: InternalRow => Any): Iterator[(Int, ColumnarBatch)] = {
      Iterators
        .wrap(
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
                // Composed batch already hold pidBatch's shared ref, so close is safe.
                ColumnarBatches.forceClose(pidBatch)
                (0, ColumnarBatches.create(ColumnarBatches.getRuntime(cb), newHandle))
            })
        .recyclePayload(p => ColumnarBatches.forceClose(p._2)) // FIXME why force close?
        .create()
    }

    val nativePartitioning: NativePartitioning = newPartitioning match {
      case SinglePartition =>
        new NativePartitioning(GlutenShuffleUtils.SinglePartitioningShortName, 1)
      case RoundRobinPartitioning(n) =>
        new NativePartitioning(GlutenShuffleUtils.RoundRobinPartitioningShortName, n)
      case HashPartitioning(exprs, n) =>
        new NativePartitioning(GlutenShuffleUtils.HashPartitioningShortName, n)
      // range partitioning fall back to row-based partition id computation
      case RangePartitioning(orders, n) =>
        new NativePartitioning(GlutenShuffleUtils.RangePartitioningShortName, n)
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
private[spark] class PartitionIdPassthrough(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}
