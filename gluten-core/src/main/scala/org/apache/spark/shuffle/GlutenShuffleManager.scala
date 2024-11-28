package org.apache.spark.shuffle

import org.apache.spark.{ShuffleDependency, SparkConf, TaskContext}

/**
 * Shuffle manager that routes shuffle API calls to different shuffle managers registered by
 * different backends.
 *
 * A SPIP may cause refactoring of this class in the future:
 * https://issues.apache.org/jira/browse/SPARK-45792
 */
class GlutenShuffleManager(conf: SparkConf, isDriver: Boolean) extends ShuffleManager {
  private val routerBuilder = ShuffleManagerRegistry.get().newRouterBuilder(conf, isDriver)

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    routerBuilder.getOrBuild().registerShuffle(shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    routerBuilder.getOrBuild().getWriter(handle, mapId, context, metrics)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    routerBuilder
      .getOrBuild()
      .getReader(handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    routerBuilder.getOrBuild().unregisterShuffle(shuffleId)
  }

  override def shuffleBlockResolver: ShuffleBlockResolver = {
    routerBuilder.getOrBuild().shuffleBlockResolver
  }

  override def stop(): Unit = {
    routerBuilder.getOrBuild().stop()
  }
}
