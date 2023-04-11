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

import io.glutenproject.columnarbatch.GlutenColumnarBatches
import io.glutenproject.memory.alloc.{NativeMemoryAllocators, Spiller}
import io.glutenproject.GlutenConfig
import io.glutenproject.vectorized._

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryConsumer
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.celeborn.RssShuffleHandle
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.shuffle.utils.CelebornPartitionPusher

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf

import java.io.IOException

class CelebornHashBasedColumnarShuffleWriter[K, V](
    handle: RssShuffleHandle[K, V, V],
    context: TaskContext,
    celebornConf: CelebornConf,
    client: ShuffleClient,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends ShuffleWriter[K, V]
  with Logging {

  private val appId: String = handle.newAppId

  private val shuffleId = handle.dependency.shuffleId

  private val numMappers = handle.numMappers

  private val numPartitions = handle.dependency.partitioner.numPartitions

  private val dep = handle.dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]]

  private val conf = SparkEnv.get.conf

  private val mapId = context.partitionId()

  private val celebornPartitionPusher = new CelebornPartitionPusher(appId, shuffleId, numMappers,
    numPartitions, context, mapId, client, celebornConf)

  private val blockManager = SparkEnv.get.blockManager
  private val offheapPerTask = GlutenConfig.getConf.taskOffHeapMemorySize
  private val nativeBufferSize = GlutenConfig.getConf.shuffleSplitDefaultSize
  private val customizedCompressionCodec = {
    val codec = GlutenConfig.getConf.columnarShuffleUseCustomizedCompressionCodec
    val enableQat = conf.getBoolean(GlutenConfig.GLUTEN_ENABLE_QAT, false) &&
      GlutenConfig.GLUTEN_QAT_SUPPORTED_CODEC.contains(codec)
    if (enableQat) {
      GlutenConfig.GLUTEN_QAT_CODEC_PREFIX + codec
    } else {
      codec
    }
  }
  private val batchCompressThreshold =
    GlutenConfig.getConf.columnarShuffleBatchCompressThreshold
  private val jniWrapper = new ShuffleSplitterJniWrapper
  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false
  private var mapStatus: MapStatus = _
  private var nativeSplitter: Long = 0

  private var splitResult: SplitResult = _

  private var partitionLengths: Array[Long] = _

  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    internalWrite(records)
  }

  @throws[IOException]
  def internalWrite(records: Iterator[Product2[K, V]]): Unit = {
    if (!records.hasNext) {
      partitionLengths = new Array[Long](dep.partitioner.numPartitions)
      client.mapperEnd(appId, shuffleId, mapId, context.attemptNumber, numMappers)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
      return
    }

    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
        val handle = GlutenColumnarBatches.getNativeHandle(cb)
        if (nativeSplitter == 0) {
          nativeSplitter = jniWrapper.makeForCeleborn(
            dep.nativePartitioning,
            offheapPerTask,
            nativeBufferSize,
            customizedCompressionCodec,
            batchCompressThreshold,
            celebornConf.pushBufferMaxSize,
            celebornPartitionPusher,
            NativeMemoryAllocators
              .createSpillable(new Spiller() {
                override def spill(size: Long, trigger: MemoryConsumer): Long = {
                  if (nativeSplitter == 0) {
                    throw new IllegalStateException(
                      "Fatal: spill() called before a shuffle splitter " +
                        "evaluator is created. This behavior should be" +
                        "optimized by moving memory " +
                        "allocations from make() to split()")
                  }
                  logInfo(s"Gluten shuffle writer: Trying to push $size bytes of data")
                  // fixme pass true when being called by self
                  val pushed = jniWrapper.nativePush(nativeSplitter, size, false)
                  logInfo(s"Gluten shuffle writer: Spilled $pushed / $size bytes of data")
                  pushed
                }
              })
              .getNativeInstanceId,
            handle,
            context.taskAttemptId()
          )
        }
        val startTime = System.nanoTime()
        val bytes = jniWrapper.split(nativeSplitter, cb.numRows, handle)
        dep.metrics("dataSize").add(bytes)
        dep.metrics("splitTime").add(System.nanoTime() - startTime)
        dep.metrics("numInputRows").add(cb.numRows)
        dep.metrics("inputBatches").add(1)
        // This metric is important, AQE use it to decide if EliminateLimit
        writeMetrics.incRecordsWritten(cb.numRows())
      }
    }

    val startTime = System.nanoTime()
    if (nativeSplitter != 0) {
      splitResult = jniWrapper.stop(nativeSplitter)
    }

    dep.metrics("splitTime").add(
      System.nanoTime() - startTime - splitResult.getTotalPushTime -
        splitResult.getTotalWriteTime -
        splitResult.getTotalCompressTime)
    writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime + splitResult.getTotalPushTime)

    partitionLengths = splitResult.getPartitionLengths

    val pushMergedDataTime = System.nanoTime
    client.prepareForMergeData(shuffleId, mapId, context.attemptNumber())
    client.pushMergedData(appId, shuffleId, mapId, context.attemptNumber)
    client.mapperEnd(appId, shuffleId, mapId, context.attemptNumber, numMappers)
    writeMetrics.incWriteTime(System.nanoTime - pushMergedDataTime)
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
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
      if (nativeSplitter != 0) {
        closeSplitter()
        nativeSplitter = 0
      }
      client.cleanup(appId, shuffleId, mapId, context.attemptNumber)
    }
  }

  def closeSplitter(): Unit = {
    jniWrapper.close(nativeSplitter)
  }

  def getPartitionLengths: Array[Long] = partitionLengths
}
