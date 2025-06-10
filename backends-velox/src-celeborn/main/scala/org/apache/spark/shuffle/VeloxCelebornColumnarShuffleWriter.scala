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
package org.apache.spark.shuffle

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.columnarbatch.ColumnarBatches
import org.apache.gluten.config.{GlutenConfig, ReservedKeys}
import org.apache.gluten.memory.memtarget.{MemoryTarget, Spiller, Spillers}
import org.apache.gluten.runtime.Runtimes
import org.apache.gluten.vectorized._

import org.apache.spark._
import org.apache.spark.memory.SparkMemoryUtil
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SparkResourceUtil

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf

import java.io.IOException

class VeloxCelebornColumnarShuffleWriter[K, V](
    shuffleId: Int,
    handle: CelebornShuffleHandle[K, V, V],
    context: TaskContext,
    celebornConf: CelebornConf,
    client: ShuffleClient,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends CelebornColumnarShuffleWriter[K, V](
    shuffleId,
    handle,
    context,
    celebornConf,
    client,
    writeMetrics) {
  private val runtime =
    Runtimes.contextInstance(BackendsApiManager.getBackendName, "CelebornShuffleWriter")

  private val partitionWriterJniWrapper = CelebornPartitionWriterJniWrapper.create(runtime)
  private val shuffleWriterJniWrapper = ShuffleWriterJniWrapper.create(runtime)

  private var splitResult: GlutenSplitResult = _

  private def availableOffHeapPerTask(): Long = {
    SparkMemoryUtil.getCurrentAvailableOffHeapMemory / SparkResourceUtil.getTaskSlots(conf)
  }

  private val nativeMetrics: SQLMetric = {
    if (dep.isSort) {
      dep.metrics("sortTime")
    } else {
      dep.metrics("splitTime")
    }
  }

  @throws[IOException]
  override def internalWrite(records: Iterator[Product2[K, V]]): Unit = {
    if (!records.hasNext) {
      handleEmptyIterator()
      return
    }
    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
        initShuffleWriter(cb)
        val columnarBatchHandle =
          ColumnarBatches.getNativeHandle(BackendsApiManager.getBackendName, cb)
        val startTime = System.nanoTime()
        shuffleWriterJniWrapper.write(
          nativeShuffleWriter,
          cb.numRows,
          columnarBatchHandle,
          availableOffHeapPerTask())
        dep.metrics("shuffleWallTime").add(System.nanoTime() - startTime)
        dep.metrics("numInputRows").add(cb.numRows)
        dep.metrics("inputBatches").add(1)
        // This metric is important, AQE use it to decide if EliminateLimit
        writeMetrics.incRecordsWritten(cb.numRows())
      }
    }

    // If all of the ColumnarBatch have empty rows, the nativeShuffleWriter still equals -1
    if (nativeShuffleWriter == -1L) {
      handleEmptyIterator()
      return
    }

    val startTime = System.nanoTime()
    splitResult = shuffleWriterJniWrapper.stop(nativeShuffleWriter)

    dep.metrics("shuffleWallTime").add(System.nanoTime() - startTime)
    nativeMetrics
      .add(
        dep.metrics("shuffleWallTime").value - splitResult.getTotalPushTime -
          splitResult.getTotalWriteTime -
          splitResult.getTotalCompressTime)
    dep.metrics("dataSize").add(splitResult.getRawPartitionLengths.sum)
    writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime + splitResult.getTotalPushTime)

    partitionLengths = splitResult.getPartitionLengths

    pushMergedDataToCeleborn()
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  override def createShuffleWriter(columnarBatch: ColumnarBatch): Unit = {
    val partitionWriterHandle = partitionWriterJniWrapper.createPartitionWriter(
      numPartitions,
      compressionCodec.orNull,
      GlutenConfig.get.columnarShuffleCodecBackend.orNull,
      compressionLevel,
      compressionBufferSize,
      clientPushBufferMaxSize,
      clientPushSortMemoryThreshold,
      celebornPartitionPusher
    )

    nativeShuffleWriter = shuffleWriterType match {
      case ReservedKeys.GLUTEN_HASH_SHUFFLE_WRITER =>
        shuffleWriterJniWrapper.createHashShuffleWriter(
          numPartitions,
          dep.nativePartitioning.getShortName,
          GlutenShuffleUtils.getStartPartitionId(dep.nativePartitioning, context.partitionId),
          nativeBufferSize,
          GlutenConfig.get.columnarShuffleReallocThreshold,
          partitionWriterHandle
        )
      case ReservedKeys.GLUTEN_RSS_SORT_SHUFFLE_WRITER =>
        shuffleWriterJniWrapper.createRssSortShuffleWriter(
          numPartitions,
          dep.nativePartitioning.getShortName,
          GlutenShuffleUtils.getStartPartitionId(dep.nativePartitioning, context.partitionId),
          nativeBufferSize,
          clientPushSortMemoryThreshold,
          compressionCodec.orNull,
          partitionWriterHandle
        )
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported celeborn shuffle writer type: $shuffleWriterType")
    }

    runtime
      .memoryManager()
      .addSpiller(new Spiller() {
        override def spill(self: MemoryTarget, phase: Spiller.Phase, size: Long): Long = {
          if (!Spillers.PHASE_SET_SPILL_ONLY.contains(phase)) {
            return 0L
          }
          logInfo(s"Gluten shuffle writer: Trying to push $size bytes of data")
          // fixme pass true when being called by self
          val pushed = shuffleWriterJniWrapper.reclaim(nativeShuffleWriter, size)
          logInfo(s"Gluten shuffle writer: Pushed $pushed / $size bytes of data")
          pushed
        }
      })
  }

  override def closeShuffleWriter(): Unit = {
    shuffleWriterJniWrapper.close(nativeShuffleWriter)
  }
}
