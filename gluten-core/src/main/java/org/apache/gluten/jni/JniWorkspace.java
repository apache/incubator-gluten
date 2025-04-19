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
package org.apache.gluten.jni;

import org.apache.gluten.exception.GlutenException;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class JniWorkspace {
  private static final Logger LOG = LoggerFactory.getLogger(JniWorkspace.class);
  private static final Map<String, JniWorkspace> INSTANCES = new ConcurrentHashMap<>();

  // Will be initialized by user code
  private static JniWorkspace DEFAULT_INSTANCE = null;
  private static final Object DEFAULT_INSTANCE_INIT_LOCK = new Object();

  // For debugging purposes only
  private static JniWorkspace DEBUG_INSTANCE = null;

  private final String workDir;
  private final JniLibLoader jniLibLoader;

  private JniWorkspace(String rootDir) {
    try {
      LOG.info("Creating JNI workspace in root directory {}", rootDir);
      Path root = Paths.get(rootDir);
      Path created = Files.createTempDirectory(root, "gluten-");
      this.workDir = created.toAbsolutePath().toString();
      this.jniLibLoader = new JniLibLoader(workDir);
      LOG.info("JNI workspace {} created in root directory {}", workDir, rootDir);
    } catch (Exception e) {
      throw new GlutenException(e);
    }
  }

  public static void enableDebug(String debugDir) {
    // Preserve the JNI libraries even after process exits.
    // This is useful for debugging native code if the debug symbols were embedded in
    // the libraries.
    synchronized (DEFAULT_INSTANCE_INIT_LOCK) {
      if (DEBUG_INSTANCE == null) {
        final File tempRoot =
            Paths.get(debugDir).resolve("gluten-jni-debug-" + UUID.randomUUID()).toFile();
        try {
          FileUtils.forceMkdir(tempRoot);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        DEBUG_INSTANCE = createOrGet(tempRoot.getAbsolutePath());
      }
      Preconditions.checkNotNull(DEBUG_INSTANCE);
      if (DEFAULT_INSTANCE == null) {
        DEFAULT_INSTANCE = DEBUG_INSTANCE;
      }
      Preconditions.checkNotNull(DEFAULT_INSTANCE);
      if (DEFAULT_INSTANCE != DEBUG_INSTANCE) {
        throw new IllegalStateException("Default instance is already set to a non-debug instance");
      }
    }
  }

  public static void initializeDefault(Supplier<String> rootDir) {
    synchronized (DEFAULT_INSTANCE_INIT_LOCK) {
      if (DEFAULT_INSTANCE == null) {
        DEFAULT_INSTANCE = createOrGet(rootDir.get());
      }
    }
  }

  public static JniWorkspace getDefault() {
    Preconditions.checkNotNull(DEFAULT_INSTANCE, "Not call initializeDefault yet");
    return DEFAULT_INSTANCE;
  }

  private static JniWorkspace createOrGet(String rootDir) {
    return INSTANCES.computeIfAbsent(rootDir, JniWorkspace::new);
  }

  public String getWorkDir() {
    return workDir;
  }

  public JniLibLoader libLoader() {
    return jniLibLoader;
  }
}
