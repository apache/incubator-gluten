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

import org.apache.spark.util.GlutenShutdownManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Function0;
import scala.runtime.BoxedUnit;

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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * LoadXXX methods in the utility prevents reloading of a library internally.
 * It's not necessary for caller
 * to manage a loaded library list.
 */
public class JniLibLoader {
  private static final Logger LOG =
      LoggerFactory.getLogger(JniLibLoader.class);

  public static Set<String> LOADED_LIBRARY_PATHS = new HashSet<>();
  public static Set<String> REQUIRE_UNLOAD_LIBRARY_PATHS = new LinkedHashSet<>();

  static {
    GlutenShutdownManager.registerUnloadLibShutdownHook(new Function0<BoxedUnit>() {
      @Override
      public BoxedUnit apply() {
        List<String> loaded = new ArrayList<>(REQUIRE_UNLOAD_LIBRARY_PATHS);
        Collections.reverse(loaded); // use reversed order to unload
        loaded.forEach(JniLibLoader::unloadFromPath);
        return BoxedUnit.UNIT;
      }
    });
  }

  private final String workDir;
  private final Set<String> loadedLibraries = new HashSet<>();
  private final Lock sync = new ReentrantLock();

  JniLibLoader(String workDir) {
    this.workDir = workDir;
  }

  private static synchronized void loadFromPath0(String libPath, boolean requireUnload) {
    if (LOADED_LIBRARY_PATHS.contains(libPath)) {
      LOG.debug("Library in path {} has already been loaded, skipping", libPath);
    } else {
      System.load(libPath);
      LOADED_LIBRARY_PATHS.add(libPath);
      LOG.info("Library {} has been loaded using path-loading method", libPath);
    }
    if (requireUnload) {
      REQUIRE_UNLOAD_LIBRARY_PATHS.add(libPath);
    }
  }

  public static void loadFromPath(String libPath) {
    loadFromPath(libPath, false);
  }

  public static void loadFromPath(String libPath, boolean requireUnload) {
    final File file = new File(libPath);
    if (!file.isFile() || !file.exists()) {
      throw new RuntimeException("library at path: " + libPath
          + " is not a file or does not exist");
    }
    loadFromPath0(file.getAbsolutePath(), requireUnload);
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

  public void loadArrowLibs() {
  }

  public JniLoadTransaction newTransaction() {
    return new JniLoadTransaction();
  }

  public static synchronized void unloadAll() {
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

  public static synchronized void unloadFromPath(String libPath) {
    LOADED_LIBRARY_PATHS.remove(libPath);

    try {
      while (Files.isSymbolicLink(Paths.get(libPath))) {
        libPath = Files.readSymbolicLink(Paths.get(libPath)).toString();
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
            String verbosePath = fs[k].get(object).toString();
            if (verbosePath.endsWith(libPath)) {
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
      loadFromPath0(libPath, false);
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
