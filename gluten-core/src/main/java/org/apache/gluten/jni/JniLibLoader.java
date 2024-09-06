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

import org.apache.spark.util.SparkShutdownManagerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import scala.runtime.BoxedUnit;

public class JniLibLoader {
  private static final Logger LOG = LoggerFactory.getLogger(JniLibLoader.class);

  private static final Set<String> LOADED_LIBRARY_PATHS =
      Collections.synchronizedSet(new HashSet<>());
  private static final Set<String> REQUIRE_UNLOAD_LIBRARY_PATHS =
      Collections.synchronizedSet(new LinkedHashSet<>());

  static {
    SparkShutdownManagerUtil.addHookForLibUnloading(
        () -> {
          forceUnloadAll();
          return BoxedUnit.UNIT;
        });
  }

  private final String workDir;
  private final Set<String> loadedLibraries = Collections.synchronizedSet(new HashSet<>());

  JniLibLoader(String workDir) {
    this.workDir = workDir;
  }

  public static void forceUnloadAll() {
    List<String> loaded;
    synchronized (REQUIRE_UNLOAD_LIBRARY_PATHS) {
      loaded = new ArrayList<>(REQUIRE_UNLOAD_LIBRARY_PATHS);
    }
    Collections.reverse(loaded); // use reversed order to unload
    loaded.forEach(JniLibLoader::unloadFromPath);
  }

  private static String toRealPath(String libPath) {
    String realPath = libPath;
    try {
      while (Files.isSymbolicLink(Paths.get(realPath))) {
        realPath = Files.readSymbolicLink(Paths.get(realPath)).toString();
      }
      LOG.info("Read real path {} for libPath {}", realPath, libPath);
      return realPath;
    } catch (Throwable th) {
      throw new GlutenException("Error to read real path for libPath: " + libPath, th);
    }
  }

  private static void loadFromPath0(String libPath, boolean requireUnload) {
    libPath = toRealPath(libPath);
    synchronized (LOADED_LIBRARY_PATHS) {
      if (LOADED_LIBRARY_PATHS.contains(libPath)) {
        LOG.debug("Library in path {} has already been loaded, skipping", libPath);
      } else {
        System.load(libPath);
        LOADED_LIBRARY_PATHS.add(libPath);
        LOG.info("Library {} has been loaded using path-loading method", libPath);
      }
    }
    if (requireUnload) {
      synchronized (REQUIRE_UNLOAD_LIBRARY_PATHS) {
        REQUIRE_UNLOAD_LIBRARY_PATHS.add(libPath);
      }
    }
  }

  public static synchronized void loadFromPath(String libPath, boolean requireUnload) {
    final File file = new File(libPath);
    if (!file.isFile() || !file.exists()) {
      throw new GlutenException("library at path: " + libPath + " is not a file or does not exist");
    }
    loadFromPath0(file.getAbsolutePath(), requireUnload);
  }

  public static void unloadFromPath(String libPath) {
    synchronized (LOADED_LIBRARY_PATHS) {
      if (!LOADED_LIBRARY_PATHS.remove(libPath)) {
        LOG.warn("Library {} was not loaded or already unloaded:", libPath);
        return;
      }
    }
    LOG.info("Starting unload library path: {} ", libPath);
    synchronized (REQUIRE_UNLOAD_LIBRARY_PATHS) {
      REQUIRE_UNLOAD_LIBRARY_PATHS.remove(libPath);
    }
    try {
      ClassLoader classLoader = JniLibLoader.class.getClassLoader();
      Field field = ClassLoader.class.getDeclaredField("nativeLibraries");
      field.setAccessible(true);
      Vector<Object> libs = (Vector<Object>) field.get(classLoader);
      synchronized (libs) {
        Iterator<Object> it = libs.iterator();
        while (it.hasNext()) {
          Object object = it.next();
          Field[] fs = object.getClass().getDeclaredFields();
          for (int k = 0; k < fs.length; k++) {
            if (fs[k].getName().equals("name")) {
              fs[k].setAccessible(true);
              String verbosePath = fs[k].get(object).toString();
              File verboseFile = new File(verbosePath);
              String verboseFileName = verboseFile.getName();
              File libFile = new File(libPath);
              String libFileName = libFile.getName();

              if (verboseFileName.equals(libFileName)) {
                LOG.info("Finalizing library file: {}", libFileName);
                Method finalize = object.getClass().getDeclaredMethod("finalize");
                finalize.setAccessible(true);
                finalize.invoke(object);
              }
            }
          }
        }
      }
    } catch (Throwable th) {
      LOG.error("Unload native library error: ", th);
    }
  }

  public void load(String libPath, boolean requireUnload) {
    synchronized (loadedLibraries) {
      try {
        if (loadedLibraries.contains(libPath)) {
          LOG.debug("Library {} has already been loaded, skipping", libPath);
          return;
        }
        File file = moveToWorkDir(workDir, libPath);
        loadWithLink(file.getAbsolutePath(), null, requireUnload);
        loadedLibraries.add(libPath);
        LOG.info("Successfully loaded library {}", libPath);
      } catch (IOException e) {
        throw new GlutenException(e);
      }
    }
  }

  public void loadAndCreateLink(String libPath, String linkName, boolean requireUnload) {
    synchronized (loadedLibraries) {
      try {
        if (loadedLibraries.contains(libPath)) {
          LOG.debug("Library {} has already been loaded, skipping", libPath);
        }
        File file = moveToWorkDir(workDir, libPath);
        loadWithLink(file.getAbsolutePath(), linkName, requireUnload);
        loadedLibraries.add(libPath);
        LOG.info("Successfully loaded library {}", libPath);
      } catch (IOException e) {
        throw new GlutenException(e);
      }
    }
  }

  private File moveToWorkDir(String workDir, String libraryToLoad) throws IOException {
    // final File temp = File.createTempFile(workDir, libraryToLoad);
    final Path libPath = Paths.get(workDir + "/" + libraryToLoad);
    if (Files.exists(libPath)) {
      Files.delete(libPath);
    }
    final File temp = new File(workDir + "/" + libraryToLoad);
    if (!temp.getParentFile().exists()) {
      temp.getParentFile().mkdirs();
    }
    try (InputStream is = JniLibLoader.class.getClassLoader().getResourceAsStream(libraryToLoad)) {
      if (is == null) {
        throw new FileNotFoundException(libraryToLoad);
      }
      try {
        Files.copy(is, temp.toPath());
      } catch (Exception e) {
        throw new GlutenException(e);
      }
    }
    return temp;
  }

  private void loadWithLink(String libPath, String linkName, boolean requireUnload)
      throws IOException {
    loadFromPath0(libPath, requireUnload);
    LOG.info("Library {} has been loaded", libPath);
    if (linkName != null) {
      Path target = Paths.get(libPath);
      Path link = Paths.get(workDir, linkName);
      if (Files.exists(link)) {
        LOG.info("Symbolic link already exists for library {}, deleting", libPath);
        Files.delete(link);
      }
      Files.createSymbolicLink(link, target);
      LOG.info("Symbolic link {} created for library {}", link, libPath);
    }
  }
}
