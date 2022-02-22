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

package com.intel.oap.vectorized;

import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/** Helper Java class for JNI Instance creation.
 * Serves as a wrapper for native library. This class is a singleton.
 * The first call to {@link #getInstance()} initiates loading of the native library.
 */
public class JniInstance {
  /** Default names for native backend, Arrow and Gandiva libraries. */
  private static final String LIBRARY_NAME = "spark_columnar_jni";
  private static final String ARROW_LIBRARY_NAME = "libarrow.so.400.0.0";
  private static final String ARROW_PARENT_LIBRARY_NAME = "libarrow.so.400";
  private static final String GANDIVA_LIBRARY_NAME = "libgandiva.so.400.0.0";
  private static final String GANDIVA_PARENT_LIBRARY_NAME = "libgandiva.so.400";
  private static boolean isLoaded = false;
  private static boolean isCodegenDependencyLoaded = false;
  private static List<String> codegenJarsLoadedCache = new ArrayList<>();
  private static volatile JniInstance INSTANCE;
  private String tmp_dir;

  public static JniInstance getInstance() throws IOException {
    String tmp_dir = System.getProperty("java.io.tmpdir");
    return getInstance(tmp_dir);
  }

  public static JniInstance getInstance(String tmp_dir) throws IOException {
    return getInstance(tmp_dir, null, null, true);
  }

  public static JniInstance getInstance(String tmp_dir, String lib_name, String libPath,
                                        boolean loadArrowAndGandiva) throws IOException {
    if (INSTANCE == null) {
      synchronized (JniInstance.class) {
        if (INSTANCE == null) {
          try {
            INSTANCE = new JniInstance(tmp_dir, lib_name, libPath, loadArrowAndGandiva);
          } catch (IllegalAccessException ex) {
            throw new IOException("IllegalAccess", ex);
          }
        }
      }
    }
    return INSTANCE;
  }

  private JniInstance(String _tmp_dir, String _lib_name, String libPath,
                      boolean loadArrowAndGandiva)
          throws IOException, IllegalAccessException, IllegalStateException {
    if (!isLoaded) {
      if (_tmp_dir.contains("nativesql")) {
        tmp_dir = _tmp_dir;
      } else {
        Path folder = Paths.get(_tmp_dir);
        Path path = Files.createTempDirectory(folder, "spark_columnar_plugin_");
        tmp_dir = path.toAbsolutePath().toString();
      }
      try {
        if (StringUtils.isNotBlank(libPath)) {
          loadLibraryFromLibPath(tmp_dir, libPath, loadArrowAndGandiva);
        } else {
          loadLibraryFromJarWithLib(tmp_dir, _lib_name, loadArrowAndGandiva);
        }
      } catch (IOException ex) {
        if (_lib_name != null) {
          System.loadLibrary(_lib_name);
        } else {
          System.loadLibrary(LIBRARY_NAME);
        }
        if (loadArrowAndGandiva) {
          System.load(ARROW_LIBRARY_NAME);
          System.load(GANDIVA_LIBRARY_NAME);
        }
      }
      isLoaded = true;
    }
  }

  public void setTempDir() throws IOException, IllegalAccessException {
    if (!isCodegenDependencyLoaded) {
      loadIncludeFromJar(tmp_dir);
      isCodegenDependencyLoaded = true;
    }
  }

  public String getTempDir() {
    return tmp_dir;
  }

  public void setJars(List<String> list_jars) throws IOException, IllegalAccessException {
    for (String jar : list_jars) {
      if (!codegenJarsLoadedCache.contains(jar)) {
        loadLibraryFromJar(jar, tmp_dir);
        codegenJarsLoadedCache.add(jar);
      }
    }
  }

  static void loadLibraryFromJar(String tmp_dir) throws IOException, IllegalAccessException {
    loadLibraryFromJarWithLib(tmp_dir, null, true);
  }

  private static void loadLibraryFromJar(String source_jar, String tmp_dir) throws IOException, IllegalAccessException {
    synchronized (JniInstance.class) {
      if (tmp_dir == null) {
        tmp_dir = System.getProperty("java.io.tmpdir");
      }
      final String folderToLoad = "";
      URL url = new URL("jar:file:" + source_jar + "!/");
      final URLConnection urlConnection = (JarURLConnection) url.openConnection();
      File tmp_dir_handler = new File(tmp_dir + "/tmp");
      if (!tmp_dir_handler.exists()) {
        tmp_dir_handler.mkdirs();
      }

      if (urlConnection instanceof JarURLConnection) {
        final JarFile jarFile = ((JarURLConnection) urlConnection).getJarFile();
        extractResourcesToDirectory(jarFile, folderToLoad, tmp_dir + "/tmp/");
      } else {
        throw new IOException(urlConnection.toString() + " is not JarUrlConnection");
      }
      /*
       * System.out.println("Current content under " + tmp_dir + "/tmp/");
       * Files.list(new File(tmp_dir + "/tmp/").toPath()).forEach(path -> {
       * System.out.println(path); });
       */
    }
  }

  /**
   * A function used to load arrow and gandiva lib from jars
   */
  static void loadArrowAndGandivaFromJarWithLib(String tmp_dir) throws IOException, IllegalAccessException {
    final File arrowlibraryFile = moveFileFromJarToTemp(tmp_dir, ARROW_LIBRARY_NAME);
    Path arrow_target = Paths.get(arrowlibraryFile.getPath());
    Path arrow_link = Paths.get(tmp_dir, ARROW_PARENT_LIBRARY_NAME);
    if (Files.exists(arrow_link)) {
      Files.delete(arrow_link);
    }
    Path symLink = Files.createSymbolicLink(arrow_link, arrow_target);
    System.load(arrowlibraryFile.getAbsolutePath());

    final File gandivalibraryFile = moveFileFromJarToTemp(tmp_dir, GANDIVA_LIBRARY_NAME);
    Path gandiva_target = Paths.get(gandivalibraryFile.getPath());
    Path gandiva_link = Paths.get(tmp_dir, GANDIVA_PARENT_LIBRARY_NAME);
    if (Files.exists(gandiva_link)) {
      Files.delete(gandiva_link);
    }
    Files.createSymbolicLink(gandiva_link, gandiva_target);
    System.load(gandivalibraryFile.getAbsolutePath());
  }

  /**
   * A function used to load an pre-installed library from the specific path.
   * **/
  static void loadLibraryFromLibPath(String tmp_dir, String libPath,
                                     boolean loadArrowAndGandiva)
          throws IOException, IllegalAccessException {
    synchronized (JniInstance.class) {
      File file = new File(libPath);
      if (!file.isFile() || !file.exists()) {
        throw new IOException("Library path: " + libPath + " is not a file or does not exist.");
      }
      System.load(file.getAbsolutePath());
      // To use columnar Shuffle, the shared library compiled by Gazelle-Jni native code
      // will be also loaded.
      final String jniLibrary = System.mapLibraryName(LIBRARY_NAME);
      final File jniLibraryFile = moveFileFromJarToTemp(tmp_dir, jniLibrary);
      System.load(jniLibraryFile.getAbsolutePath());
      if (loadArrowAndGandiva) {
        loadArrowAndGandivaFromJarWithLib(tmp_dir);
      }
    }
  }

  static void loadLibraryFromJarWithLib(String tmp_dir, String lib_name,
                                        boolean loadArrowAndGandiva) throws IOException, IllegalAccessException {
    synchronized (JniInstance.class) {
      if (tmp_dir == null) {
        tmp_dir = System.getProperty("java.io.tmpdir");
      }

      final String libraryToLoad;
      if (lib_name != null) {
        libraryToLoad = System.mapLibraryName(lib_name);
      } else {
        libraryToLoad = System.mapLibraryName(LIBRARY_NAME);
      }
      final File libraryFile = moveFileFromJarToTemp(tmp_dir, libraryToLoad);
      System.load(libraryFile.getAbsolutePath());

      if (loadArrowAndGandiva) {
        loadArrowAndGandivaFromJarWithLib(tmp_dir);
      }
    }
  }

  private static void loadIncludeFromJar(String tmp_dir) throws IOException, IllegalAccessException {
    synchronized (JniInstance.class) {
      if (tmp_dir == null) {
        tmp_dir = System.getProperty("java.io.tmpdir");
      }
      final String folderToLoad = "include";
      final URLConnection urlConnection = JniInstance.class.getClassLoader().getResource("include").openConnection();
      if (urlConnection instanceof JarURLConnection) {
        final JarFile jarFile = ((JarURLConnection) urlConnection).getJarFile();
        extractResourcesToDirectory(jarFile, folderToLoad, tmp_dir + "/" + "nativesql_include");
      } else {
        // For Maven test only
        String path = urlConnection.getURL().toString();
        if (urlConnection.getURL().toString().startsWith("file:")) {
          // remove the prefix of "file:" from includePath
          path = urlConnection.getURL().toString().substring(5);
        }
        final File folder = new File(path);
        copyResourcesToDirectory(urlConnection,
                                 tmp_dir + "/" + "nativesql_include", folder);
      }
    }
  }

  private static File moveFileFromJarToTemp(String tmpDir, String libraryToLoad) throws IOException {
    // final File temp = File.createTempFile(tmpDir, libraryToLoad);
    Path lib_path = Paths.get(tmpDir + "/" + libraryToLoad);
    if (Files.exists(lib_path)) {
      return new File(tmpDir + "/" + libraryToLoad);
    }
    final File temp = new File(tmpDir + "/" + libraryToLoad);
    try (final InputStream is = JniInstance.class.getClassLoader().getResourceAsStream(libraryToLoad)) {
      if (is == null) {
        throw new FileNotFoundException(libraryToLoad);
      }
      try {
        Files.copy(is, temp.toPath());
      } catch (Exception e) {
      }
    }
    return temp;
  }

  public static void extractResourcesToDirectory(JarFile origJar, String jarPath, String destPath) throws IOException {
    for (Enumeration<JarEntry> entries = origJar.entries(); entries.hasMoreElements();) {
      JarEntry oneEntry = entries.nextElement();
      if (((jarPath == "" && !oneEntry.getName().contains("META-INF")) || (oneEntry.getName().startsWith(jarPath + "/")))
          && !oneEntry.isDirectory()) {
        int rm_length = jarPath.length() == 0 ? 0 : jarPath.length() + 1;
        Path dest_path = Paths.get(destPath + "/" + oneEntry.getName().substring(rm_length));
        if (Files.exists(dest_path)) {
          continue;
        }
        File destFile = new File(destPath + "/" + oneEntry.getName().substring(rm_length));
        File parentFile = destFile.getParentFile();
        if (parentFile != null) {
          parentFile.mkdirs();
        }

        FileOutputStream outFile = new FileOutputStream(destFile);
        InputStream inFile = origJar.getInputStream(oneEntry);

        try {
          byte[] buffer = new byte[4 * 1024];

          int s = 0;
          while ((s = inFile.read(buffer)) > 0) {
            outFile.write(buffer, 0, s);
          }
        } catch (IOException e) {
          throw new IOException("Could not extract resource from jar", e);
        } finally {
          try {
            inFile.close();
          } catch (IOException ignored) {
          }
          try {
            outFile.close();
          } catch (IOException ignored) {
          }
        }
      }
    }
  }

  public static void copyResourcesToDirectory(URLConnection urlConnection,
                                              String destPath, File folder) throws IOException {
    for (final File fileEntry : Objects.requireNonNull(folder.listFiles())) {
      String destFilePath = destPath + "/" + fileEntry.getName();
      File destFile = new File(destFilePath);
      if (fileEntry.isDirectory()) {
        destFile.mkdirs();
        copyResourcesToDirectory(urlConnection, destFilePath, fileEntry);
      } else {
        try {
          Files.copy(fileEntry.toPath(), destFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
        }
      }
    }
  }
}
