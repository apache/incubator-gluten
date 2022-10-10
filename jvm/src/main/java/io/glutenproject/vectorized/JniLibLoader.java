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

package io.glutenproject.vectorized;

import io.glutenproject.GlutenConfig;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Function0;
import scala.runtime.BoxedUnit;

import org.apache.spark.util.GlutenShutdownManager;

/**
 * LoadXXX methods in the utility prevents reloading of a library internally.
 * It's not necessary for caller
 * to manage a loaded library list.
 */
public class JniLibLoader {
  private static final Logger LOG =
      LoggerFactory.getLogger(JniLibLoader.class);
  public static String BACKEND_NAME = "";

  public static Set<String> LOADED_LIBRARY_PATHS = new HashSet<>();

  private final String workDir;
  private final Set<String> loadedLibraries = new HashSet<>();
  private final Lock sync = new ReentrantLock();

  JniLibLoader(String workDir) {
    this.workDir = workDir;
  }

  private static synchronized void loadFromPath0(String libPath) {
    if (LOADED_LIBRARY_PATHS.contains(libPath)) {
      LOG.debug("Library in path {} has already been loaded, skipping", libPath);
      return;
    }
    System.load(libPath);
    LOADED_LIBRARY_PATHS.add(libPath);
    LOG.info("Library {} has been loaded using path-loading method", libPath);
    // Unload native lib.
    GlutenShutdownManager.registerUnloadLibShutdownHook(new Function0<BoxedUnit>() {
      @Override
      public BoxedUnit apply() {
        JniLibLoader.unloadNativeLibs(libPath);
        return BoxedUnit.UNIT;
      }
    });
  }

  public static void loadFromPath(String libPath) {
    final File file = new File(libPath);
    if (!file.isFile() || !file.exists()) {
      throw new RuntimeException("library at path: " + libPath
          + " is not a file or does not exist");
    }
    loadFromPath0(file.getAbsolutePath());
  }

  public void mapAndLoad(String unmappedLibName) {
    newTransaction()
        .mapAndLoad(unmappedLibName)
        .commit();
  }

  public void load(String libName) {
    newTransaction()
        .load(libName)
        .commit();
  }

  public void loadAndCreateLink(String libName, String linkName) {
    newTransaction()
        .loadAndCreateLink(libName, linkName)
        .commit();
  }

  public void loadEssentials() {
    loadArrowLibs();
    loadGlutenLib();
  }

  public void loadArrowLibs() {
    if (BACKEND_NAME.equals(GlutenConfig.GLUTEN_GAZELLE_BACKEND())) {
      LOG.info("Load gazelle backend arrow lib");
      newTransaction()
          .loadAndCreateLink("libarrow.so.800.0.0", "libarrow.so.800")
          .loadAndCreateLink("libgandiva.so.800.0.0", "libgandiva.so.800")
          .loadAndCreateLink("libparquet.so.800.0.0", "libparquet.so.800")
          .loadAndCreateLink("libarrow_dataset.so.800.0.0", "libarrow_dataset.so.800")
          .loadAndCreateLink("libarrow_substrait.so.800.0.0", "libarrow_substrait.so.800")
          .commit();
    } else {
      LOG.info("Load other backend arrow lib");
      newTransaction()
          .loadAndCreateLink("libarrow.so.1000.0.0", "libarrow.so.1000")
          .loadAndCreateLink("libgandiva.so.1000.0.0", "libgandiva.so.1000")
          .commit();
    }
  }

  public void loadGlutenLib() {
    newTransaction()
        .mapAndLoad("spark_columnar_jni")
        .commit();
  }

  public JniLoadTransaction newTransaction() {
    return new JniLoadTransaction();
  }

  public static synchronized void unloadAllNativeLibs() {
    LOADED_LIBRARY_PATHS.clear();
    try {
      ClassLoader classLoader = JniLibLoader.class.getClassLoader();
      Field field = ClassLoader.class.getDeclaredField("nativeLibraries");
      field.setAccessible(true);
      Vector<Object> libs = (Vector<Object>) field.get(classLoader);
      Iterator it = libs.iterator();
      while (it.hasNext()) {
        Object object = it.next();
        Method finalize = object.getClass().getDeclaredMethod("finalize");
        finalize.setAccessible(true);
        finalize.invoke(object);
      }
    } catch (Throwable th) {
      LOG.error("Unload native library error: ", th);
    }
  }

  public static synchronized void unloadNativeLibs(String libName) {
    LOADED_LIBRARY_PATHS.remove(libName);

    try {
      while (Files.isSymbolicLink(Paths.get(libName))) {
        libName = Files.readSymbolicLink(Paths.get(libName)).toString();
      }

      ClassLoader classLoader = JniLibLoader.class.getClassLoader();
      Field field = ClassLoader.class.getDeclaredField("nativeLibraries");
      field.setAccessible(true);
      Vector<Object> libs = (Vector<Object>) field.get(classLoader);
      Iterator it = libs.iterator();
      while (it.hasNext()) {
        Object object = it.next();
        Field[] fs = object.getClass().getDeclaredFields();
        for (int k = 0; k < fs.length; k++) {
          if (fs[k].getName().equals("name")) {
            fs[k].setAccessible(true);
            String dllPath = fs[k].get(object).toString();
            if (dllPath.endsWith(libName)) {
              Method finalize = object.getClass().getDeclaredMethod("finalize");
              finalize.setAccessible(true);
              finalize.invoke(object);
            }
          }
        }
      }
    } catch (Throwable th) {
      LOG.error("Unload native library error: ", th);
    }
  }

  private static final class LoadRequest {
    final String libName;
    final String linkName;
    final File file;

    private LoadRequest(String libName, String linkName, File file) {
      this.libName = libName;
      this.linkName = linkName;
      this.file = file;
    }

    LoadRequest(String libName, File file) {
      this(libName, null, file);
    }

    public boolean requireLinking() {
      return !Objects.isNull(linkName);
    }
  }

  public class JniLoadTransaction {
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final Map<String, String> toLoad = new LinkedHashMap<>(); // ordered

    private JniLoadTransaction() {
      JniLibLoader.this.sync.lock();
    }

    public JniLoadTransaction mapAndLoad(String unmappedLibName) {
      try {
        final String mappedLibName = System.mapLibraryName(unmappedLibName);
        load(mappedLibName);
        return this;
      } catch (Exception e) {
        abort();
        throw new RuntimeException(e);
      }
    }

    public JniLoadTransaction load(String libName) {
      try {
        toLoad.put(libName, null);
        return this;
      } catch (Exception e) {
        abort();
        throw new RuntimeException(e);
      }
    }

    public JniLoadTransaction loadAndCreateLink(String libName, String linkName) {
      try {
        toLoad.put(libName, linkName);
        return this;
      } catch (Exception e) {
        abort();
        throw new RuntimeException(e);
      }
    }

    public void commit() {
      try {
        terminate();
        toLoad.entrySet().stream()
            .flatMap(e -> {
              try {
                final String libName = e.getKey();
                final String linkName = e.getValue();
                if (loadedLibraries.contains(libName)) {
                  LOG.debug("Library {} has already been loaded, skipping", libName);
                  return Stream.empty();
                }
                // load only libraries not loaded yet
                final File file = moveToWorkDir(workDir, libName);
                return Stream.of(new LoadRequest(libName, linkName, file));
              } catch (IOException ex) {
                throw new RuntimeException(ex);
              }
            })
            .collect(Collectors.toList())
            .forEach(e -> {
              try {
                LOG.info("Trying to load library {}", e.libName);
                loadWithLink(workDir, e);
                loadedLibraries.add(e.libName);
                LOG.info("Successfully loaded library {}", e.libName);
              } catch (Exception ex) {
                throw new RuntimeException(ex);
              }
            });
      } finally {
        JniLibLoader.this.sync.unlock();
      }
    }

    public void abort() {
      try {
        terminate();
        // do nothing as of now
      } finally {
        JniLibLoader.this.sync.unlock();
      }
    }

    private void terminate() {
      if (!finished.compareAndSet(false, true)) {
        throw new IllegalStateException();
      }
    }

    private File moveToWorkDir(String workDir, String libraryToLoad) throws IOException {
      // final File temp = File.createTempFile(workDir, libraryToLoad);
      final Path libPath = Paths.get(workDir + "/" + libraryToLoad);
      if (Files.exists(libPath)) {
        Files.delete(libPath);
      }
      final File temp = new File(workDir + "/" + libraryToLoad);
      try (InputStream is = JniLibLoader.class.getClassLoader()
          .getResourceAsStream(libraryToLoad)) {
        if (is == null) {
          throw new FileNotFoundException(libraryToLoad);
        }
        try {
          Files.copy(is, temp.toPath());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
      return temp;
    }

    private void loadWithLink(String workDir, LoadRequest req) throws IOException {
      String libPath = req.file.getAbsolutePath();
      loadFromPath0(libPath);
      LOG.info("Library {} has been loaded", libPath);
      if (!req.requireLinking()) {
        LOG.debug("Symbolic link not required for library {}, skipping", libPath);
        return;
      }
      // create link
      Path arrowTarget = Paths.get(req.file.getPath());
      Path arrowLink = Paths.get(workDir, req.linkName);
      if (Files.exists(arrowLink)) {
        LOG.info("Symbolic link already exists for library {}, deleting", libPath);
        Files.delete(arrowLink);
      }
      Files.createSymbolicLink(arrowLink, arrowTarget);
      LOG.info("Symbolic link {} created for library {}", arrowLink, libPath);
    }

  }

}
