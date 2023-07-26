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

package io.glutenproject.init;

import io.glutenproject.GlutenConfig;
import io.glutenproject.backendsapi.BackendsApiManager;
import org.apache.spark.sql.internal.SQLConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

// Initialize global contexts before calling any native methods from Java side.
public abstract class JniInitialized {

  private static final Logger LOG = LoggerFactory.getLogger(JniInitialized.class);

  static {
    try {
      String prefix = BackendsApiManager.getSettings().getBackendConfigPrefix();
      Map<String, String> nativeConfMap =
          GlutenConfig.getNativeBackendConf(prefix, SQLConf.get().getAllConfs());
      InitializerJniWrapper.initialize(JniUtils.toNativeConf(nativeConfMap));
    } catch (Exception e) {
      LOG.error("Error calling InitializerJniWrapper.initialize(...)", e);
      throw e;
    }
  }

  protected JniInitialized() {}
}
