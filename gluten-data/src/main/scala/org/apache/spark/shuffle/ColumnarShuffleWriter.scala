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
import io.glutenproject.columnarbatch.ColumnarBatches
import io.glutenproject.memory.Spiller
import io.glutenproject.memory.alloc.NativeMemoryAllocators
import io.glutenproject.vectorized._
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SHUFFLE_COMPRESS
import org.apache.spark.memory.{MemoryConsumer, SparkMemoryUtil}
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.{SparkDirectoryUtil, SparkResourcesUtil, Utils}

import java.io.IOException

class ColumnarShuffleWriter[K, V](
    shuffleBlockResolver: IndexShuffleBlockResolver,
    handle: BaseShuffleHandle[K, V, V],
    mapId: Long,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends ShuffleWriter[K, V]
  with Logging {

  private val dep = handle.dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]]

  private val conf = SparkEnv.get.conf

  private val blockManager = SparkEnv.get.blockManager

  // Are we in the process of stopping? Because map tasks can call stop() with success = true
  // and then call stop() with success = false if they get an exception, we want to make sure
  // we don't try deleting files, etc twice.
  private var stopping = false

  private var mapStatus: MapStatus = _

  private val localDirs = SparkDirectoryUtil
    .namespace("shuffle-write")
    .all
    .map(_.getAbsolutePath)
    .mkString(",")

  private val nativeBufferSize = GlutenConfig.getConf.maxBatchSize

  private val compressionCodec =
    if (conf.getBoolean(SHUFFLE_COMPRESS.key, SHUFFLE_COMPRESS.defaultValue.get)) {
      GlutenShuffleUtils.getCompressionCodec(conf)
    } else {
      null // uncompressed
    }

  private val compressionCodecBackend =
    GlutenConfig.getConf.columnarShuffleCodecBackend.orNull

  private val batchCompressThreshold =
    GlutenConfig.getConf.columnarShuffleBatchCompressThreshold

  private val preferSpill = GlutenConfig.getConf.columnarShufflePreferSpill

  private val writeSchema = GlutenConfig.getConf.columnarShuffleWriteSchema

  private val jniWrapper = new ShuffleWriterJniWrapper

  private var nativeShuffleWriter: Long = -1L

  private var splitResult: SplitResult = _

  private var partitionLengths: Array[Long] = _

  private var rawPartitionLengths: Array[Long] = _

  private val taskContext: TaskContext = TaskContext.get()

  private def availableOffHeapPerTask(): Long = {
    // FIXME Is this calculation always reliable ? E.g. if dynamic allocation is enabled
    val executorCores = SparkResourcesUtil.getExecutorCores(conf)
    val taskCores = conf.getInt("spark.task.cpus", 1)
    val perTask =
      SparkMemoryUtil.getCurrentAvailableOffHeapMemory / (executorCores / taskCores)
    perTask
  }

  @throws[IOException]
  def internalWrite(records: Iterator[Product2[K, V]]): Unit = {
    if (!records.hasNext) {
      partitionLengths = new Array[Long](dep.partitioner.numPartitions)
      shuffleBlockResolver.writeMetadataFileAndCommit(
        dep.shuffleId,
        mapId,
        partitionLengths,
        Array[Long](),
        null)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
      return
    }

    val dataTmp = Utils.tempFileWith(shuffleBlockResolver.getDataFile(dep.shuffleId, mapId))

    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
        val handle = ColumnarBatches.getNativeHandle(cb)
        if (nativeShuffleWriter == -1L) {
          nativeShuffleWriter = jniWrapper.make(
            dep.nativePartitioning,
            availableOffHeapPerTask(),
            nativeBufferSize,
            compressionCodec,
            compressionCodecBackend,
            batchCompressThreshold,
            dataTmp.getAbsolutePath,
            blockManager.subDirsPerLocalDir,
            localDirs,
            preferSpill,
            NativeMemoryAllocators
              .getDefault()
              .createSpillable(new Spiller() {
                override def spill(size: Long, trigger: MemoryConsumer): Long = {
                  if (nativeShuffleWriter == -1L) {
                    throw new IllegalStateException(
                      "Fatal: spill() called before a shuffle writer " +
                        "is created. This behavior should be optimized by moving memory " +
                        "allocations from make() to split()")
                  }
                  logInfo(s"Gluten shuffle writer: Trying to spill $size bytes of data")
                  // fixme pass true when being called by self
                  val spilled = jniWrapper.nativeEvict(nativeShuffleWriter, size, false)
                  logInfo(s"Gluten shuffle writer: Spilled $spilled / $size bytes of data")
                  spilled
                }
              })
              .getNativeInstanceId,
            writeSchema,
            handle,
            taskContext.taskAttemptId()
          )
        }
        val startTime = System.nanoTime()
        val bytes = jniWrapper.split(nativeShuffleWriter, cb.numRows, handle)
        dep.metrics("dataSize").add(bytes)
        dep.metrics("splitTime").add(System.nanoTime() - startTime)
        dep.metrics("numInputRows").add(cb.numRows)
        dep.metrics("inputBatches").add(1)
        // This metric is important, AQE use it to decide if EliminateLimit
        writeMetrics.incRecordsWritten(cb.numRows())
      }
    }

    val startTime = System.nanoTime()
    if (nativeShuffleWriter != -1L) {
      splitResult = jniWrapper.stop(nativeShuffleWriter)
    }

    dep
      .metrics("splitTime")
      .add(
        System.nanoTime() - startTime - splitResult.getTotalSpillTime -
          splitResult.getTotalWriteTime -
          splitResult.getTotalCompressTime)
    dep.metrics("spillTime").add(splitResult.getTotalSpillTime)
    dep.metrics("compressTime").add(splitResult.getTotalCompressTime)
    dep.metrics("bytesSpilled").add(splitResult.getTotalBytesSpilled)
    writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime + splitResult.getTotalSpillTime)

    partitionLengths = splitResult.getPartitionLengths
    rawPartitionLengths = splitResult.getRawPartitionLengths
    try {
      shuffleBlockResolver.writeMetadataFileAndCommit(
        dep.shuffleId,
        mapId,
        partitionLengths,
        Array[Long](),
        dataTmp)
    } finally {
      if (dataTmp.exists() && !dataTmp.delete()) {
        logError(s"Error while deleting temp file ${dataTmp.getAbsolutePath}")
      }
    }

    // The partitionLength is much more than vanilla spark partitionLengths,
    // almost 3 times than vanilla spark partitionLengths
    // This value is sensitive in rules such as AQE rule OptimizeSkewedJoin DynamicJoinSelection
    // May affect the final plan
    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    internalWrite(records)
  }

  def closeShuffleWriter(): Unit = {
    jniWrapper.close(nativeShuffleWriter)
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
    }
  }

  def getPartitionLengths: Array[Long] = partitionLengths

}
