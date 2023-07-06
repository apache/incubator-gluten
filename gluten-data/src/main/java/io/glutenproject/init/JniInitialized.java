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
import org.apache.spark.util.memory.TaskResourceManager;
import org.apache.spark.util.memory.TaskResources;

import java.util.Map;

// Initialize global / local contexts before calling any native methods from Java side.
public abstract class JniInitialized {
  static {
    String prefix = BackendsApiManager.getSettings().getBackendConfigPrefix();
    Map<String, String> nativeConfMap = GlutenConfig.getNativeBackendConf(
        prefix, SQLConf.get().getAllConfs());
    InitializerJniWrapper.initialize(JniUtils.toNativeConf(nativeConfMap));
  }

  protected JniInitialized() {
    if (!TaskResources.inSparkTask()) {
      return;
    }

    if (!TaskResources.isResourceManagerRegistered(TaskContextManager.RESOURCE_ID)) {
      TaskResources.addResourceManager(TaskContextManager.RESOURCE_ID, new TaskContextManager());
    }
  }

  protected long getBackendHandle() {
    if (!TaskResources.isResourceManagerRegistered(TaskContextManager.RESOURCE_ID)) {
      throw new IllegalStateException("Resource not registered: " + TaskContextManager.RESOURCE_ID);
    }
    return ((TaskContextManager) TaskResources.getResourceManager(
        TaskContextManager.RESOURCE_ID)).handle;
  }

  // manages lifecycles of native thread-local task contexts
  private static class TaskContextManager implements TaskResourceManager {
    private static final String RESOURCE_ID = TaskContextManager.class.toString();
    private final long handle;

    private TaskContextManager() {
      handle = InitializerJniWrapper.makeTaskContext();
    }

    @Override
    public void release() throws Exception {
      InitializerJniWrapper.closeTaskContext(handle);
    }

    @Override
    public long priority() {
      return TaskResourceManager.super.priority();
    }
  }
}
