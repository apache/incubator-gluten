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

import java.io.IOException

import com.google.common.annotations.VisibleForTesting
import com.intel.oap.GazellePluginConfig
import com.intel.oap.expression.ConverterUtils
import com.intel.oap.spark.sql.execution.datasources.v2.arrow.Spiller
import com.intel.oap.vectorized.{ArrowWritableColumnVector, ShuffleSplitterJniWrapper, SplitResult}
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.memory.MemoryConsumer
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.sql.execution.datasources.v2.arrow.SparkMemoryUtils
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

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

  private val localDirs = blockManager.diskBlockManager.localDirs.mkString(",")
  private val nativeBufferSize =
    conf.getInt("spark.sql.execution.arrow.maxRecordsPerBatch", 4096)

  private val customizedCompressCodec =
    GazellePluginConfig.getConf.columnarShuffleUseCustomizedCompressionCodec
  private val defaultCompressionCodec = if (conf.getBoolean("spark.shuffle.compress", true)) {
    conf.get("spark.io.compression.codec", "lz4")
  } else {
    "uncompressed"
  }
  private val preferSpill = GazellePluginConfig.getConf.columnarShufflePreferSpill

  private val jniWrapper = new ShuffleSplitterJniWrapper()

  private var nativeSplitter: Long = 0

  private var splitResult: SplitResult = _

  private var partitionLengths: Array[Long] = _

  private var firstRecordBatch: Boolean = true

  @throws[IOException]
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    if (!records.hasNext) {
      partitionLengths = new Array[Long](dep.partitioner.numPartitions)
      shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId, mapId, partitionLengths, null)
      mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
      return
    }

    val dataTmp = Utils.tempFileWith(shuffleBlockResolver.getDataFile(dep.shuffleId, mapId))
    if (nativeSplitter == 0) {
      nativeSplitter = jniWrapper.make(
        dep.nativePartitioning,
        nativeBufferSize,
        defaultCompressionCodec,
        dataTmp.getAbsolutePath,
        blockManager.subDirsPerLocalDir,
        localDirs,
        preferSpill,
        SparkMemoryUtils.createSpillableMemoryPool(
          new Spiller() {
            override def spill(size: Long, trigger: MemoryConsumer): Long = {
              if (nativeSplitter == 0) {
                throw new IllegalStateException("Fatal: spill() called before a shuffle splitter " +
                  "evaluator is created. This behavior should be optimized by moving memory " +
                  "allocations from make() to split()")
              }
              // fixme pass true when being called by self
              return jniWrapper.nativeSpill(nativeSplitter, size, false)
            }
          }).getNativeInstanceId)
    }

    while (records.hasNext) {
      val cb = records.next()._2.asInstanceOf[ColumnarBatch]
      if (cb.numRows == 0 || cb.numCols == 0) {
        logInfo(s"Skip ColumnarBatch of ${cb.numRows} rows, ${cb.numCols} cols")
      } else {
        val bufAddrs = new ListBuffer[Long]()
        val bufSizes = new ListBuffer[Long]()
        val recordBatch = ConverterUtils.createArrowRecordBatch(cb)
        recordBatch.getBuffers().asScala.foreach { buffer => bufAddrs += buffer.memoryAddress() }
        recordBatch.getBuffersLayout().asScala.foreach { bufLayout =>
          bufSizes += bufLayout.getSize()
        }
        dep.dataSize.add(bufSizes.sum)

        val startTime = System.nanoTime()
        val existingIntType: Boolean = if (firstRecordBatch) {
          // Check whether the recordbatch contain the Int data type.
          val arrowSchema = ConverterUtils.getSchemaFromBytesBuf(dep.nativePartitioning.getSchema)
          import scala.collection.JavaConverters._
          arrowSchema.getFields.asScala.find(_.getType.getTypeID == ArrowTypeID.Int).nonEmpty
        } else false

        // Choose the compress type based on the compress size of the first record batch.
        if (firstRecordBatch && conf.getBoolean("spark.shuffle.compress", true) &&
          customizedCompressCodec != defaultCompressionCodec && existingIntType) {
          // Compute the default compress size
          jniWrapper.setCompressType(nativeSplitter, defaultCompressionCodec)
          val defaultCompressedSize = jniWrapper.split(
            nativeSplitter, cb.numRows, bufAddrs.toArray, bufSizes.toArray, firstRecordBatch)

          // Compute the custom compress size.
          jniWrapper.setCompressType(nativeSplitter, customizedCompressCodec)
          val customizedCompressedSize = jniWrapper.split(
            nativeSplitter, cb.numRows, bufAddrs.toArray, bufSizes.toArray, firstRecordBatch)

          // Choose the compress algorithm based on the compress size.
          if (customizedCompressedSize != -1 && defaultCompressedSize != -1) {
            if (customizedCompressedSize > defaultCompressedSize) {
              jniWrapper.setCompressType(nativeSplitter, defaultCompressionCodec)
            }
          } else {
            logError("Failed to compute the compress size in the first record batch")
          }
        }
        firstRecordBatch = false

        jniWrapper.split(nativeSplitter, cb.numRows, bufAddrs.toArray, bufSizes.toArray, firstRecordBatch)
        dep.splitTime.add(System.nanoTime() - startTime)
        dep.numInputRows.add(cb.numRows)
        writeMetrics.incRecordsWritten(1)
        ConverterUtils.releaseArrowRecordBatch(recordBatch)
      }
    }

    val startTime = System.nanoTime()
    splitResult = jniWrapper.stop(nativeSplitter)

    dep.splitTime.add(System
      .nanoTime() - startTime - splitResult.getTotalSpillTime - splitResult.getTotalWriteTime - splitResult.getTotalComputePidTime - splitResult.getTotalCompressTime)
    dep.spillTime.add(splitResult.getTotalSpillTime)
    dep.compressTime.add(splitResult.getTotalCompressTime)
    dep.computePidTime.add(splitResult.getTotalComputePidTime)
    dep.bytesSpilled.add(splitResult.getTotalBytesSpilled)
    writeMetrics.incBytesWritten(splitResult.getTotalBytesWritten)
    writeMetrics.incWriteTime(splitResult.getTotalWriteTime + splitResult.getTotalSpillTime)

    partitionLengths = splitResult.getPartitionLengths
    try {
      shuffleBlockResolver.writeIndexFileAndCommit(
        dep.shuffleId,
        mapId,
        partitionLengths,
        dataTmp)
    } finally {
      if (dataTmp.exists() && !dataTmp.delete()) {
        logError(s"Error while deleting temp file ${dataTmp.getAbsolutePath}")
      }
    }
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
        jniWrapper.close(nativeSplitter)
        nativeSplitter = 0
      }
    }
  }

  @VisibleForTesting
  def getPartitionLengths: Array[Long] = partitionLengths

}
