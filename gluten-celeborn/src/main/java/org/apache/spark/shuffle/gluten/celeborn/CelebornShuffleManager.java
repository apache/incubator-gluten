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
package org.apache.spark.shuffle.gluten.celeborn;

import io.glutenproject.exception.GlutenException;

import org.apache.celeborn.client.LifecycleManager;
import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.celeborn.common.identity.UserIdentifier;
import org.apache.celeborn.common.protocol.ShuffleMode;
import org.apache.spark.*;
import org.apache.spark.shuffle.*;
import org.apache.spark.shuffle.celeborn.CelebornShuffleFallbackPolicyRunner;
import org.apache.spark.shuffle.celeborn.CelebornShuffleHandle;
import org.apache.spark.shuffle.celeborn.CelebornShuffleReader;
import org.apache.spark.shuffle.celeborn.SparkUtils;
import org.apache.spark.shuffle.sort.ColumnarShuffleManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

public class CelebornShuffleManager implements ShuffleManager {

  private static final Logger logger = LoggerFactory.getLogger(CelebornShuffleManager.class);

  private static final String GLUTEN_SHUFFLE_MANAGER_NAME =
      "org.apache.spark.shuffle.sort.ColumnarShuffleManager";

  private static final String LOCAL_SHUFFLE_READER_KEY =
      "spark.sql.adaptive.localShuffleReader.enabled";

  private final SparkConf conf;
  private final CelebornConf celebornConf;
  // either be "{appId}_{appAttemptId}" or "{appId}"
  private String appUniqueId;

  private LifecycleManager lifecycleManager;
  private ShuffleClient shuffleClient;
  private volatile ColumnarShuffleManager _columnarShuffleManager;
  private final ConcurrentHashMap.KeySetView<Integer, Boolean> columnarShuffleIds =
      ConcurrentHashMap.newKeySet();
  private final CelebornShuffleFallbackPolicyRunner fallbackPolicyRunner;

  public CelebornShuffleManager(SparkConf conf) {
    if (conf.getBoolean(LOCAL_SHUFFLE_READER_KEY, true)) {
      logger.warn(
          "Detected {} (default is true) is enabled, it's highly recommended to disable it when "
              + "use Celeborn as Remote Shuffle Service to avoid performance degradation.",
          LOCAL_SHUFFLE_READER_KEY);
    }
    this.conf = conf;
    this.celebornConf = SparkUtils.fromSparkConf(conf);
    this.fallbackPolicyRunner = new CelebornShuffleFallbackPolicyRunner(celebornConf);
  }

  private boolean isDriver() {
    return "driver".equals(SparkEnv.get().executorId());
  }

  private ColumnarShuffleManager columnarShuffleManager() {
    if (_columnarShuffleManager == null) {
      synchronized (this) {
        if (_columnarShuffleManager == null) {
          _columnarShuffleManager =
              SparkUtils.instantiateClass(GLUTEN_SHUFFLE_MANAGER_NAME, conf, isDriver());
        }
      }
    }
    return _columnarShuffleManager;
  }

  private void initializeLifecycleManager() {
    // Only create LifecycleManager singleton in Driver. When register shuffle multiple times, we
    // need to ensure that LifecycleManager will only be created once. Parallelism needs to be
    // considered in this place, because if there is one RDD that depends on multiple RDDs
    // at the same time, it may bring parallel `register shuffle`, such as Join in Sql.
    if (isDriver() && lifecycleManager == null) {
      synchronized (this) {
        if (lifecycleManager == null) {
          lifecycleManager = new LifecycleManager(appUniqueId, celebornConf);
          try {
            try {
              Method method =
                  // for Celeborn 0.3.1 and above, see CELEBORN-804
                  ShuffleClient.class.getDeclaredMethod(
                      "get",
                      String.class,
                      String.class,
                      int.class,
                      CelebornConf.class,
                      UserIdentifier.class);
              shuffleClient =
                  (ShuffleClient)
                      method.invoke(
                          null,
                          appUniqueId,
                          lifecycleManager.getHost(),
                          Integer.valueOf(lifecycleManager.getPort()),
                          celebornConf,
                          lifecycleManager.getUserIdentifier());
            } catch (NoSuchMethodException noMethod) {
              Method method =
                  // for Celeborn 0.3.0, see CELEBORN-798
                  ShuffleClient.class.getDeclaredMethod(
                      "get",
                      String.class,
                      String.class,
                      int.class,
                      CelebornConf.class,
                      UserIdentifier.class,
                      boolean.class);
              shuffleClient =
                  (ShuffleClient)
                      method.invoke(
                          null,
                          appUniqueId,
                          lifecycleManager.getHost(),
                          Integer.valueOf(lifecycleManager.getPort()),
                          celebornConf,
                          lifecycleManager.getUserIdentifier(),
                          Boolean.TRUE);
            }
          } catch (ReflectiveOperationException rethrow) {
            throw new RuntimeException(rethrow);
          }
        }
      }
    }
  }

  @Override
  public <K, V, C> ShuffleHandle registerShuffle(
      int shuffleId, ShuffleDependency<K, V, C> dependency) {
    // Note: generate app unique id at driver side, make sure dependency.rdd.context
    // is the same SparkContext among different shuffleIds.
    // This method may be called many times.
    if (dependency instanceof ColumnarShuffleDependency) {
      appUniqueId = SparkUtils.appUniqueId(dependency.rdd().context());
      initializeLifecycleManager();

      if (fallbackPolicyRunner.applyAllFallbackPolicy(
          lifecycleManager, dependency.partitioner().numPartitions())) {
        logger.warn("Fallback to ColumnarShuffleManager!");
        columnarShuffleIds.add(shuffleId);
        return columnarShuffleManager().registerShuffle(shuffleId, dependency);
      } else {
        return new CelebornShuffleHandle<>(
            appUniqueId,
            lifecycleManager.getHost(),
            lifecycleManager.getPort(),
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
    if (appUniqueId == null) {
      return true;
    }
    if (shuffleClient == null) {
      return false;
    }
    return shuffleClient.unregisterShuffle(shuffleId, isDriver());
  }

  @Override
  public ShuffleBlockResolver shuffleBlockResolver() {
    return columnarShuffleManager().shuffleBlockResolver();
  }

  @Override
  public void stop() {
    if (shuffleClient != null) {
      shuffleClient.shutdown();
      ShuffleClient.reset();
      shuffleClient = null;
    }
    if (lifecycleManager != null) {
      lifecycleManager.stop();
      lifecycleManager = null;
    }
    if (columnarShuffleManager() != null) {
      columnarShuffleManager().stop();
      _columnarShuffleManager = null;
    }
  }

  @Override
  public <K, V> ShuffleWriter<K, V> getWriter(
      ShuffleHandle handle, long mapId, TaskContext context, ShuffleWriteMetricsReporter metrics) {
    try {
      if (handle instanceof CelebornShuffleHandle) {
        @SuppressWarnings("unchecked")
        CelebornShuffleHandle<K, V, V> h = ((CelebornShuffleHandle<K, V, V>) handle);
        ShuffleClient client =
            ShuffleClient.get(
                h.appUniqueId(),
                h.lifecycleManagerHost(),
                h.lifecycleManagerPort(),
                celebornConf,
                h.userIdentifier(),
                false);
        if (ShuffleMode.HASH.equals(celebornConf.shuffleWriterMode())) {
          return new CelebornHashBasedColumnarShuffleWriter<>(
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
      throw new GlutenException(e);
    }
  }

  // Added in SPARK-32055, for Spark 3.1 and above
  public <K, C> ShuffleReader<K, C> getReader(
      ShuffleHandle handle,
      int startMapIndex,
      int endMapIndex,
      int startPartition,
      int endPartition,
      TaskContext context,
      ShuffleReadMetricsReporter metrics) {
    if (handle instanceof CelebornShuffleHandle) {
      @SuppressWarnings("unchecked")
      CelebornShuffleHandle<K, ?, C> h = (CelebornShuffleHandle<K, ?, C>) handle;
      return new CelebornShuffleReader<>(
          h,
          startPartition,
          endPartition,
          startMapIndex,
          endMapIndex,
          context,
          celebornConf,
          metrics);
    }
    return columnarShuffleManager()
        .getReader(
            handle, startMapIndex, endMapIndex, startPartition, endPartition, context, metrics);
  }
}
