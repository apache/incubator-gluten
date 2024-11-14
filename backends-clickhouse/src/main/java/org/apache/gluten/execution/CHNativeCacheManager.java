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
package org.apache.gluten.execution;

import java.util.Set;

public class CHNativeCacheManager {
  public static String cacheParts(String table, Set<String> columns, boolean onlyMetaCache) {
    return nativeCacheParts(table, String.join(",", columns), onlyMetaCache);
  }

  private static native String nativeCacheParts(
      String table, String columns, boolean onlyMetaCache);

  public static CacheResult getCacheStatus(String jobId) {
    return nativeGetCacheStatus(jobId);
  }

  private static native CacheResult nativeGetCacheStatus(String jobId);

  public static native String nativeCacheFiles(byte[] files);

  // only for ut
  public static native void removeFiles(String file, String cacheName);
}
