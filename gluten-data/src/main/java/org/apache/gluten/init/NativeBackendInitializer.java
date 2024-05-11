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
package org.apache.gluten.init;

import org.apache.gluten.GlutenConfig;
import org.apache.gluten.backendsapi.BackendsApiManager;

import org.apache.spark.util.GlutenShutdownManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.runtime.BoxedUnit;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

// Initialize native backend before calling any native methods from Java side.
public final class NativeBackendInitializer {
  private static final Logger LOG = LoggerFactory.getLogger(NativeBackendInitializer.class);
  private static final AtomicBoolean initialized = new AtomicBoolean(false);

  // Spark DriverPlugin/ExecutorPlugin will only invoke NativeBackendInitializer#initializeBackend
  // method once in its init method.
  // In cluster mode, NativeBackendInitializer#initializeBackend only will be invoked in different
  // JVM.
  // In local mode, NativeBackendInitializer#initializeBackend will be invoked twice in same
  // thread, driver first then executor, initialized flag ensure only invoke initializeBackend once,
  // so there are no race condition here.
  public static void initializeBackend(scala.collection.Map<String, String> conf) {
    if (!initialized.compareAndSet(false, true)) {
      // Already called.
      return;
    }
    initialize0(conf);
    GlutenShutdownManager.addHook(() -> {
      shutdown();
      return BoxedUnit.UNIT;
    });
  }

  private static void initialize0(scala.collection.Map<String, String> conf) {
    try {
      String prefix = BackendsApiManager.getSettings().getBackendConfigPrefix();
      Map<String, String> nativeConfMap = GlutenConfig.getNativeBackendConf(prefix, conf);
      BackendsApiManager.getSettings().resolveNativeConf(nativeConfMap);
      initialize(JniUtils.toNativeConf(nativeConfMap));
    } catch (Exception e) {
      LOG.error("Failed to call native backend's initialize method", e);
      throw e;
    }
  }

  private static native void initialize(byte[] configPlan);

  private static native void shutdown();

  private NativeBackendInitializer() {
  }
}
