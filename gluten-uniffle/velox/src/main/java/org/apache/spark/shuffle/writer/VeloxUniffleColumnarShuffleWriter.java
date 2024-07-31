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
package org.apache.spark.shuffle.writer;

import org.apache.gluten.GlutenConfig;
import org.apache.gluten.columnarbatch.ColumnarBatches;
import org.apache.gluten.exec.Runtime;
import org.apache.gluten.exec.Runtimes;
import org.apache.gluten.memory.memtarget.MemoryTarget;
import org.apache.gluten.memory.memtarget.Spiller;
import org.apache.gluten.memory.memtarget.Spillers;
import org.apache.gluten.vectorized.ShuffleWriterJniWrapper;
import org.apache.gluten.vectorized.SplitResult;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.SparkMemoryUtil;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.shuffle.ColumnarShuffleDependency;
import org.apache.spark.shuffle.GlutenShuffleUtils;
import org.apache.spark.shuffle.RssShuffleHandle;
import org.apache.spark.shuffle.RssShuffleManager;
import org.apache.spark.shuffle.RssSparkConfig;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.util.SparkResourceUtil;
import org.apache.uniffle.client.api.ShuffleWriteClient;
import org.apache.uniffle.common.ShuffleBlockInfo;
import org.apache.uniffle.common.exception.RssException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import scala.Option;
import scala.Product2;
import scala.collection.Iterator;

public class VeloxUniffleColumnarShuffleWriter<K, V> extends RssShuffleWriter<K, V, V> {

  private static final Logger LOG =
      LoggerFactory.getLogger(VeloxUniffleColumnarShuffleWriter.class);

  private long nativeShuffleWriter = -1L;

  private boolean stopping = false;
  private final int compressThreshold =
      GlutenConfig.getConf().columnarShuffleCompressionThreshold();
  private final double reallocThreshold = GlutenConfig.getConf().columnarShuffleReallocThreshold();
  private String compressionCodec;
  private final int compressionLevel;
  private final int partitionId;

  private final Runtime runtime = Runtimes.contextInstance("UniffleShuffleWriter");
  private final ShuffleWriterJniWrapper jniWrapper = ShuffleWriterJniWrapper.create(runtime);
  private final int nativeBufferSize = GlutenConfig.getConf().maxBatchSize();
  private final int bufferSize;
  private final Boolean isSort;

  private final ColumnarShuffleDependency<K, V, V> columnarDep;
  private final SparkConf sparkConf;

  private long availableOffHeapPerTask() {
    return SparkMemoryUtil.getCurrentAvailableOffHeapMemory()
        / SparkResourceUtil.getTaskSlots(sparkConf);
  }

  public VeloxUniffleColumnarShuffleWriter(
      int partitionId,
      String appId,
      int shuffleId,
      String taskId,
      long taskAttemptId,
      ShuffleWriteMetrics shuffleWriteMetrics,
      RssShuffleManager shuffleManager,
      SparkConf sparkConf,
      ShuffleWriteClient shuffleWriteClient,
      RssShuffleHandle<K, V, V> rssHandle,
      Function<String, Boolean> taskFailureCallback,
      TaskContext context,
      Boolean isSort) {
    super(
        appId,
        shuffleId,
        taskId,
        taskAttemptId,
        shuffleWriteMetrics,
        shuffleManager,
        sparkConf,
        shuffleWriteClient,
        rssHandle,
        taskFailureCallback,
        context);
    columnarDep = (ColumnarShuffleDependency<K, V, V>) rssHandle.getDependency();
    this.partitionId = partitionId;
    this.sparkConf = sparkConf;
    this.isSort = isSort;
    bufferSize =
        (int)
            sparkConf.getSizeAsBytes(
                RssSparkConfig.RSS_WRITER_BUFFER_SIZE.key(),
                RssSparkConfig.RSS_WRITER_BUFFER_SIZE.defaultValue().get());
    if ((boolean) sparkConf.get(package$.MODULE$.SHUFFLE_COMPRESS())) {
      compressionCodec = GlutenShuffleUtils.getCompressionCodec(sparkConf);
    }
    compressionLevel = GlutenShuffleUtils.getCompressionLevel(sparkConf, compressionCodec, null);
  }

  @Override
  protected void writeImpl(Iterator<Product2<K, V>> records) {
    if (!records.hasNext() && !isMemoryShuffleEnabled) {
      super.sendCommit();
      return;
    }
    // writer already init
    PartitionPusher partitionPusher = new PartitionPusher(this);
    while (records.hasNext()) {
      ColumnarBatch cb = (ColumnarBatch) (records.next()._2());
      if (cb.numRows() == 0 || cb.numCols() == 0) {
        LOG.info("Skip ColumnarBatch of 0 rows or 0 cols");
      } else {
        long handle = ColumnarBatches.getNativeHandle(cb);
        if (nativeShuffleWriter == -1) {
          nativeShuffleWriter =
              jniWrapper.makeForRSS(
                  columnarDep.nativePartitioning(),
                  nativeBufferSize,
                  // use field do this
                  compressionCodec,
                  compressionLevel,
                  compressThreshold,
                  GlutenConfig.getConf().columnarShuffleCompressionMode(),
                  (int) (long) sparkConf.get(package$.MODULE$.SHUFFLE_SORT_INIT_BUFFER_SIZE()),
                  (boolean) sparkConf.get(package$.MODULE$.SHUFFLE_SORT_USE_RADIXSORT()),
                  bufferSize,
                  bufferSize,
                  partitionPusher,
                  handle,
                  taskAttemptId,
                  GlutenShuffleUtils.getStartPartitionId(
                      columnarDep.nativePartitioning(), partitionId),
                  "uniffle",
                  isSort
                      ? GlutenConfig.GLUTEN_SORT_SHUFFLE_WRITER()
                      : GlutenConfig.GLUTEN_HASH_SHUFFLE_WRITER(),
                  reallocThreshold);
          runtime.addSpiller(
              new Spiller() {
                @Override
                public long spill(MemoryTarget self, Spiller.Phase phase, long size) {
                  if (!Spillers.PHASE_SET_SPILL_ONLY.contains(phase)) {
                    return 0L;
                  }
                  LOG.info("Gluten shuffle writer: Trying to push {} bytes of data", size);
                  long pushed = jniWrapper.nativeEvict(nativeShuffleWriter, size, false);
                  LOG.info("Gluten shuffle writer: Pushed {} / {} bytes of data", pushed, size);
                  return pushed;
                }
              });
        }
        long startTime = System.nanoTime();
        long bytes =
            jniWrapper.write(nativeShuffleWriter, cb.numRows(), handle, availableOffHeapPerTask());
        LOG.debug("jniWrapper.write rows {}, split bytes {}", cb.numRows(), bytes);
        columnarDep.metrics().get("dataSize").get().add(bytes);
        // this metric replace part of uniffle shuffle write time
        columnarDep.metrics().get("shuffleWallTime").get().add(System.nanoTime() - startTime);
        columnarDep.metrics().get("numInputRows").get().add(cb.numRows());
        columnarDep.metrics().get("inputBatches").get().add(1);
        shuffleWriteMetrics.incRecordsWritten(cb.numRows());
      }
    }

    long startTime = System.nanoTime();
    LOG.info("nativeShuffleWriter value {}", nativeShuffleWriter);
    if (nativeShuffleWriter == -1L) {
      throw new IllegalStateException("nativeShuffleWriter should not be -1L");
    }
    SplitResult splitResult;
    try {
      splitResult = jniWrapper.stop(nativeShuffleWriter);
    } catch (IOException e) {
      throw new RssException(e);
    }
    columnarDep.metrics().get("shuffleWallTime").get().add(System.nanoTime() - startTime);
    columnarDep
        .metrics()
        .get("splitTime")
        .get()
        .add(
            columnarDep.metrics().get("shuffleWallTime").get().value()
                - splitResult.getTotalPushTime()
                - splitResult.getTotalWriteTime()
                - splitResult.getTotalCompressTime());

    shuffleWriteMetrics.incBytesWritten(splitResult.getTotalBytesWritten());
    shuffleWriteMetrics.incWriteTime(
        splitResult.getTotalWriteTime() + splitResult.getTotalPushTime());
    // partitionLengths is calculate in uniffle side

    long pushMergedDataTime = System.nanoTime();
    // clear all
    sendRestBlockAndWait();
    if (!isMemoryShuffleEnabled) {
      super.sendCommit();
    }
    long writeDurationMs = System.nanoTime() - pushMergedDataTime;
    shuffleWriteMetrics.incWriteTime(writeDurationMs);
    LOG.info(
        "Finish write shuffle with rest write {} ms",
        TimeUnit.MILLISECONDS.toNanos(writeDurationMs));
  }

  @Override
  public Option<MapStatus> stop(boolean success) {
    if (!stopping) {
      stopping = true;
      closeShuffleWriter();
      return super.stop(success);
    }
    return Option.<MapStatus>empty();
  }

  private void closeShuffleWriter() {
    if (nativeShuffleWriter != -1) {
      jniWrapper.close(nativeShuffleWriter);
      nativeShuffleWriter = -1;
    }
  }

  private void sendRestBlockAndWait() {
    List<ShuffleBlockInfo> shuffleBlockInfos = super.getBufferManager().clear();
    super.processShuffleBlockInfos(shuffleBlockInfos);
    // make checkBlockSendResult no arguments
    super.internalCheckBlockSendResult();
  }

  public int doAddByte(int partitionId, byte[] data, int length) {
    List<ShuffleBlockInfo> shuffleBlockInfos =
        super.getBufferManager()
            .addPartitionData(partitionId, data, length, System.currentTimeMillis());
    super.processShuffleBlockInfos(shuffleBlockInfos);
    return length;
  }
}
