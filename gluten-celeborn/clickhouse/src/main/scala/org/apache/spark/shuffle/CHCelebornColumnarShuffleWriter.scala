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

import org.apache.gluten.GlutenConfig
import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.vectorized._

import org.apache.spark._
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle
import org.apache.spark.sql.vectorized.ColumnarBatch

import org.apache.celeborn.client.ShuffleClient
import org.apache.celeborn.common.CelebornConf
import org.apache.celeborn.common.protocol.ShuffleMode

import java.io.IOException
import java.util.Locale

class CHCelebornColumnarShuffleWriter[K, V](
    shuffleId: Int,
    handle: CelebornShuffleHandle[K, V, V],
    context: TaskContext,
    celebornConf: CelebornConf,
    client: ShuffleClient,
    writeMetrics: ShuffleWriteMetricsReporter)
  extends CelebornColumnarShuffleWriter[K, V](
    shuffleId: Int,
    handle,
    context,
    celebornConf,
    client,
    writeMetrics) {

  private val customizedCompressCodec =
    customizedCompressionCodec.toUpperCase(Locale.ROOT)

  private val jniWrapper = new CHShuffleSplitterJniWrapper

  private var splitResult: CHSplitResult = _

  @throws[IOException]
  override def internalWrite(records: Iterator[Product2[K, V]]): Unit = {
    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
        initShuffleWriter(cb)
        val col = cb.column(0).asInstanceOf[CHColumnVector]
        val startTime = System.nanoTime()
        jniWrapper.split(nativeShuffleWriter, col.getBlockAddress)
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
    splitResult = jniWrapper.stop(nativeShuffleWriter)

    dep.metrics("shuffleWallTime").add(System.nanoTime() - startTime)
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
    pushMergedDataToCeleborn()
    mapStatus = MapStatus(blockManager.shuffleServerId, splitResult.getRawPartitionLengths, mapId)
  }

  override def createShuffleWriter(columnarBatch: ColumnarBatch): Unit = {
    nativeShuffleWriter = jniWrapper.makeForRSS(
      dep.nativePartitioning,
      shuffleId,
      mapId,
      nativeBufferSize,
      customizedCompressCodec,
      compressionLevel,
      GlutenConfig.getConf.chColumnarShuffleSpillThreshold,
      CHBackendSettings.shuffleHashAlgorithm,
      celebornPartitionPusher,
      GlutenConfig.getConf.chColumnarForceMemorySortShuffle
        || ShuffleMode.SORT.name.equalsIgnoreCase(shuffleWriterType)
    )
  }

  override def closeShuffleWriter(): Unit = {
    jniWrapper.close(nativeShuffleWriter)
  }
}
