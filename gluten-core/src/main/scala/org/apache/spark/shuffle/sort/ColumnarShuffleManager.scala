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
package org.apache.spark.shuffle.sort

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle._
import org.apache.spark.shuffle.sort.SortShuffleManager.canUseBatchFetch
import org.apache.spark.storage.BlockId
import org.apache.spark.util.collection.OpenHashSet

import java.io.InputStream
import java.util.concurrent.ConcurrentHashMap

class ColumnarShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  import ColumnarShuffleManager._

  private[this] lazy val sortShuffleManager: SortShuffleManager = new SortShuffleManager(conf)

  override val shuffleBlockResolver = new IndexShuffleBlockResolver(conf)

  /** A mapping from shuffle ids to the number of mappers producing output for those shuffles. */
  private[this] val taskIdMapsForShuffle = new ConcurrentHashMap[Int, OpenHashSet[Long]]()

  /** Obtains a [[ShuffleHandle]] to pass to tasks. */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    if (dependency.isInstanceOf[ColumnarShuffleDependency[_, _, _]]) {
      logInfo(s"Registering ColumnarShuffle shuffleId: $shuffleId")
      new ColumnarShuffleHandle[K, V](
        shuffleId,
        dependency.asInstanceOf[ColumnarShuffleDependency[K, V, V]])
    } else {
      // Otherwise call default SortShuffleManager
      sortShuffleManager.registerShuffle(shuffleId, dependency)
    }
  }

  /** Get a writer for a given partition. Called on executors by map tasks. */
  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    handle match {
      case columnarShuffleHandle: ColumnarShuffleHandle[K @unchecked, V @unchecked] =>
        val mapTaskIds =
          taskIdMapsForShuffle.computeIfAbsent(handle.shuffleId, _ => new OpenHashSet[Long](16))
        mapTaskIds.synchronized {
          mapTaskIds.add(context.taskAttemptId())
        }
        GlutenShuffleWriterWrapper.genColumnarShuffleWriter(
          shuffleBlockResolver,
          columnarShuffleHandle,
          mapId,
          metrics)
      case _ => sortShuffleManager.getWriter(handle, mapId, context, metrics)
    }
  }

  /**
   * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
   * Called on executors by reduce tasks.
   */
  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    if (handle.isInstanceOf[ColumnarShuffleHandle[_, _]]) {
      val (blocksByAddress, canEnableBatchFetch) = {
        GlutenShuffleUtils.getReaderParam(
          handle,
          startMapIndex,
          endMapIndex,
          startPartition,
          endPartition)
      }
      val shouldBatchFetch =
        canEnableBatchFetch && canUseBatchFetch(startPartition, endPartition, context)
      new BlockStoreShuffleReader(
        handle.asInstanceOf[BaseShuffleHandle[K, _, C]],
        blocksByAddress,
        context,
        metrics,
        serializerManager = bypassDecompressionSerializerManger,
        shouldBatchFetch = shouldBatchFetch
      )
    } else {
      sortShuffleManager.getReader(
        handle,
        startMapIndex,
        endMapIndex,
        startPartition,
        endPartition,
        context,
        metrics)
    }
  }

  /** Remove a shuffle's metadata from the ShuffleManager. */
  override def unregisterShuffle(shuffleId: Int): Boolean = {
    if (taskIdMapsForShuffle.contains(shuffleId)) {
      Option(taskIdMapsForShuffle.remove(shuffleId)).foreach {
        mapTaskIds =>
          mapTaskIds.iterator.foreach {
            mapId => shuffleBlockResolver.removeDataByMap(shuffleId, mapId)
          }
      }
      true
    } else {
      sortShuffleManager.unregisterShuffle(shuffleId)
    }
  }

  /** Shut down this ShuffleManager. */
  override def stop(): Unit = {
    if (!taskIdMapsForShuffle.isEmpty) {
      shuffleBlockResolver.stop()
    } else {
      sortShuffleManager.stop
    }
  }
}

object ColumnarShuffleManager extends Logging {
  private def bypassDecompressionSerializerManger =
    new SerializerManager(
      SparkEnv.get.serializer,
      SparkEnv.get.conf,
      SparkEnv.get.securityManager.getIOEncryptionKey()) {
      // Bypass the shuffle read decompression, decryption is not supported
      override def wrapStream(blockId: BlockId, s: InputStream): InputStream = {
        s
      }
    }
}

private[spark] class ColumnarShuffleHandle[K, V](
    shuffleId: Int,
    dependency: ShuffleDependency[K, V, V])
  extends BaseShuffleHandle(shuffleId, dependency) {}
