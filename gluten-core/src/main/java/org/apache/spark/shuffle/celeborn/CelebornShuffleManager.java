package org.apache.spark.shuffle.celeborn;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.protocol.ShuffleMode;

import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.*;
import org.apache.spark.shuffle.sort.ColumnarShuffleManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class CelebornShuffleManager implements ShuffleManager {

  private static final Logger logger = LoggerFactory.getLogger(CelebornShuffleManager.class);

  private static final String glutenShuffleManagerName =
    "org.apache.spark.shuffle.sort.ColumnarShuffleManager";

  private final SparkConf conf;
  private final CelebornConf celebornConf;
  private final ConcurrentHashMap.KeySetView<Integer, Boolean> columnarShuffleIds =
    ConcurrentHashMap.newKeySet();
  private final RssShuffleFallbackPolicyRunner fallbackPolicyRunner;
  private String newAppId;
  private LifecycleManager lifecycleManager;
  private ShuffleClient rssShuffleClient;
  private volatile ColumnarShuffleManager _columnarShuffleManager;

  public CelebornShuffleManager(SparkConf conf) {
    this.conf = conf;
    this.celebornConf = SparkUtils.fromSparkConf(conf);
    this.fallbackPolicyRunner = new RssShuffleFallbackPolicyRunner(celebornConf);
  }

  private boolean isDriver() {
    return "driver".equals(SparkEnv.get().executorId());
  }

  private ColumnarShuffleManager columnarShuffleManager() {
    if (_columnarShuffleManager == null) {
      synchronized (this) {
        if (_columnarShuffleManager == null) {
          _columnarShuffleManager =
            SparkUtils.instantiateClass(glutenShuffleManagerName, conf, isDriver());
        }
      }
    }
    return _columnarShuffleManager;
  }

  private void initializeLifecycleManager(String appId) {
    // Only create LifecycleManager singleton in Driver.
    // When register shuffle multiple times, we
    // need to ensure that LifecycleManager will only be created once.
    // Parallelism needs to be considered in this place,
    // because if there is one RDD that depends on multiple RDDs
    // at the same time, it may bring parallel `register shuffle`, such as Join in Sql.
    if (isDriver() && lifecycleManager == null) {
      synchronized (this) {
        if (lifecycleManager == null) {
          lifecycleManager = new LifecycleManager(appId, celebornConf);
          rssShuffleClient =
            ShuffleClient.get(
              lifecycleManager.self(), celebornConf, lifecycleManager.getUserIdentifier());
        }
      }
    }
  }

  @Override
  public <K, V, C> ShuffleHandle registerShuffle(
    int shuffleId, ShuffleDependency<K, V, C> dependency) {
    // Note: generate newAppId at driver side, make sure dependency.rdd.context
    // is the same SparkContext among different shuffleIds.
    // This method may be called many times.
    if (dependency instanceof ColumnarShuffleDependency) {
      newAppId = SparkUtils.genNewAppId(dependency.rdd().context());
      initializeLifecycleManager(newAppId);

      if (fallbackPolicyRunner.applyAllFallbackPolicy(
        lifecycleManager, dependency.partitioner().numPartitions())) {
        logger.warn("Fallback to ColumnarShuffleManager!");
        columnarShuffleIds.add(shuffleId);

        return columnarShuffleManager().registerShuffle(shuffleId, dependency);
      } else {
        return new RssShuffleHandle<>(
          newAppId,
          lifecycleManager.getRssMetaServiceHost(),
          lifecycleManager.getRssMetaServicePort(),
          lifecycleManager.getUserIdentifier(),
          shuffleId,
          dependency.rdd().getNumPartitions(),
          dependency);
      }
    }
    return columnarShuffleManager().registerShuffle(shuffleId, dependency);
  }

  @Override
  public boolean unregisterShuffle(int shuffleId) {
    if (columnarShuffleIds.contains(shuffleId)) {
      return columnarShuffleManager().unregisterShuffle(shuffleId);
    }
    if (newAppId == null) {
      return true;
    }
    if (rssShuffleClient == null) {
      return false;
    }
    return rssShuffleClient.unregisterShuffle(newAppId, shuffleId, isDriver());
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    return columnarShuffleManager().shuffleBlockResolver();
  }

  @Override
  public void stop() {
    if (rssShuffleClient != null) {
      rssShuffleClient.shutdown();
    }
    if (lifecycleManager != null) {
      lifecycleManager.stop();
    }
    if (columnarShuffleManager() != null) {
      columnarShuffleManager().stop();
    }
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
    ShuffleHandle handle, long mapId, TaskContext context, ShuffleWriteMetricsReporter metrics) {
    try {
      if (handle instanceof RssShuffleHandle) {
        @SuppressWarnings("unchecked")
        RssShuffleHandle<K, V, V> h = ((RssShuffleHandle<K, V, V>) handle);
        ShuffleClient client =
          ShuffleClient.get(
            h.rssMetaServiceHost(), h.rssMetaServicePort(), celebornConf, h.userIdentifier());
        if (ShuffleMode.HASH.equals(celebornConf.shuffleWriterMode())) {
          return CelebornShuffleWriterWrapper
            .genCelebornColumnarShuffleWriter(ShuffleMode.HASH.toString(),
            h, context, celebornConf, client, metrics);
        } else {
          throw new UnsupportedOperationException(
            "Unrecognized shuffle write mode!" + celebornConf.shuffleWriterMode());
        }
      } else {
        columnarShuffleIds.add(handle.shuffleId());
        return columnarShuffleManager().getWriter(handle, mapId, context, metrics);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public <K, C> ShuffleReader<K, C> getReader(
    ShuffleHandle handle,
    int startMapIndex,
    int endMapIndex,
    int startPartition,
    int endPartition,
    TaskContext context,
    ShuffleReadMetricsReporter metrics) {
    if (handle instanceof RssShuffleHandle) {
      @SuppressWarnings("unchecked")
      RssShuffleHandle<K, ?, C> h = (RssShuffleHandle<K, ?, C>) handle;
      return new RssShuffleReader<>(
        h,
        startPartition,
        endPartition,
        startMapIndex,
        endMapIndex,
        context,
        celebornConf,
        metrics);
    }
    return columnarShuffleManager().getReader(
      handle,
      startMapIndex,
      endMapIndex,
      startPartition,
      endPartition,
      context,
      metrics);
  }
}

