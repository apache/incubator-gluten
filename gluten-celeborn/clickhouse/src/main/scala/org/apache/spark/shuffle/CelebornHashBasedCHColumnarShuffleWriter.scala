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

import io.glutenproject.GlutenConfig
import io.glutenproject.memory.alloc.CHNativeMemoryAllocators
import io.glutenproject.memory.memtarget.spark.Spiller
import io.glutenproject.vectorized._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf

import java.io.IOException
import java.util.Locale

class CelebornHashBasedCHColumnarShuffleWriter[K, V](
    handle: CelebornShuffleHandle[K, V, V],
    context: TaskContext,
    celebornConf: CelebornConf,
    client: ShuffleClient,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends ShuffleWriter[K, V]
  with Logging {

  private val shuffleId = handle.dependency.shuffleId

  private val numMappers = handle.numMappers

  private val numPartitions = handle.dependency.partitioner.numPartitions

  private val dep = handle.dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]]

  private val conf = SparkEnv.get.conf

  private val mapId = context.partitionId()

  private val celebornPartitionPusher = new CelebornPartitionPusher(
    shuffleId,
    numMappers,
    numPartitions,
    context,
    mapId,
    client,
    celebornConf)

  private val blockManager = SparkEnv.get.blockManager

  private val nativeBufferSize = GlutenConfig.getConf.maxBatchSize
  private val customizedCompressCodec =
    GlutenShuffleUtils.getCompressionCodec(conf).toUpperCase(Locale.ROOT)

  private val jniWrapper = new CHShuffleSplitterJniWrapper
  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false
  private var mapStatus: MapStatus = _
  private var nativeShuffleWriter: Long = -1L

  private var splitResult: CHSplitResult = _

  private var partitionLengths: Array[Long] = _

  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    internalWrite(records)
  }

  @throws[IOException]
  def internalWrite(records: Iterator[Product2[K, V]]): Unit = {
    if (!records.hasNext) {
      partitionLengths = new Array[Long](dep.partitioner.numPartitions)
      client.mapperEnd(shuffleId, mapId, context.attemptNumber, numMappers)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
      return
    }

    if (nativeShuffleWriter == -1L) {
      nativeShuffleWriter = jniWrapper.makeForRSS(
        dep.nativePartitioning,
        shuffleId,
        mapId,
        nativeBufferSize,
        customizedCompressCodec,
        GlutenConfig.getConf.chColumnarShuffleSpillThreshold,
        celebornPartitionPusher
      )
      CHNativeMemoryAllocators.createSpillable(
        "CelebornShuffleWriter",
        new Spiller() {
          override def spill(size: Long): Long = {
            if (nativeShuffleWriter == -1L) {
              throw new IllegalStateException(
                "Fatal: spill() called before a celeborn shuffle writer " +
                  "is created. This behavior should be" +
                  "optimized by moving memory " +
                  "allocations from make() to split()")
            }
            logInfo(s"Gluten shuffle writer: Trying to push $size bytes of data")
            val spilled = jniWrapper.evict(nativeShuffleWriter);
            logInfo(s"Gluten shuffle writer: Spilled $spilled / $size bytes of data")
            spilled
          }
        }
      )
    }
    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
        val col = cb.column(0).asInstanceOf[CHColumnVector]
        val block = col.getBlockAddress
        jniWrapper
          .split(nativeShuffleWriter, block)
        dep.metrics("numInputRows").add(cb.numRows)
        dep.metrics("inputBatches").add(1)
        // This metric is important, AQE use it to decide if EliminateLimit
        writeMetrics.incRecordsWritten(cb.numRows())
      }
    }

    splitResult = jniWrapper.stop(nativeShuffleWriter)

    dep.metrics("splitTime").add(splitResult.getSplitTime)
    dep.metrics("IOTime").add(splitResult.getDiskWriteTime)
    dep.metrics("serializeTime").add(splitResult.getSerializationTime)
    dep.metrics("spillTime").add(splitResult.getTotalSpillTime)
    dep.metrics("compressTime").add(splitResult.getTotalCompressTime)
    dep.metrics("computePidTime").add(splitResult.getTotalComputePidTime)
    dep.metrics("bytesSpilled").add(splitResult.getTotalBytesSpilled)
    dep.metrics("dataSize").add(splitResult.getTotalBytesWritten)
    writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime + splitResult.getTotalSpillTime)

    partitionLengths = splitResult.getPartitionLengths
    val pushMergedDataTime = System.nanoTime
    client.prepareForMergeData(shuffleId, mapId, context.attemptNumber())
    client.pushMergedData(shuffleId, mapId, context.attemptNumber)
    client.mapperEnd(shuffleId, mapId, context.attemptNumber, numMappers)
    writeMetrics.incWriteTime(System.nanoTime - pushMergedDataTime)
    mapStatus = MapStatus(blockManager.shuffleServerId, splitResult.getRawPartitionLengths, mapId)
  }

  override def stop(success: Boolean): Option[MapStatus] = {
    try {
      if (stopping) {
        None
      }
      stopping = true
      if (success) {
        Option(mapStatus)
      } else {
        None
      }
    } finally {
      if (nativeShuffleWriter != -1L) {
        closeShuffleWriter()
        nativeShuffleWriter = -1L
      }
      client.cleanup(shuffleId, mapId, context.attemptNumber)
    }
  }

  def closeShuffleWriter(): Unit = {
    jniWrapper.close(nativeShuffleWriter)
  }

  def getPartitionLengths: Array[Long] = partitionLengths
}
