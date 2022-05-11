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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class JniResourceHelper {
  private static final Logger LOG =
      LoggerFactory.getLogger(JniResourceHelper.class);

  private final String workDir;
  private final Set<String> jarExtracted = new HashSet<>();

  private boolean headersExtracted = false;

  JniResourceHelper(String workDir) {
    this.workDir = workDir;
  }

  public synchronized void extractHeaders() {
    try {
      if (headersExtracted) {
        LOG.debug("Headers already extracted to work directory {}, skipping", workDir);
        return;
      }
      LOG.info("Trying to extract headers to work directory {}", workDir);
      extractHeaders0(workDir);
      LOG.info("Successfully extracted headers to work directory {}", workDir);
      headersExtracted = true;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized void extractJars(List<String> jars) {
    jars.stream()
        .filter(jar -> {
          if (jarExtracted.contains(jar)) {
            LOG.debug("Jar {} already extracted to work directory {}, skipping", jar, workDir);
            return false;
          }
          return true;
        })
        .forEach(jar -> {
          try {
            LOG.info("Trying to extract jar {} to work directory {}", jar, workDir);
            extractJar(jar, workDir);
            LOG.info("Successfully extracted jar {} to work directory {}", jar, workDir);
            jarExtracted.add(jar);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private static void extractHeaders0(String workDir) throws IOException, IllegalAccessException {
    if (workDir == null) {
      workDir = System.getProperty("java.io.tmpdir");
    }
    final String folderToLoad = "include";
    final URLConnection urlConnection = JniResourceHelper.class.getClassLoader().getResource("include").openConnection();
    if (urlConnection instanceof JarURLConnection) {
      final JarFile jarFile = ((JarURLConnection) urlConnection).getJarFile();
      extractResourcesToDirectory(jarFile, folderToLoad, workDir + "/" + "nativesql_include");
    } else {
      // For Maven test only
      String path = urlConnection.getURL().toString();
      if (urlConnection.getURL().toString().startsWith("file:")) {
        // remove the prefix of "file:" from includePath
        path = urlConnection.getURL().toString().substring(5);
      }
      final File folder = new File(path);
      copyResourcesToDirectory(urlConnection,
          workDir + "/" + "nativesql_include", folder);
    }
  }

  private static void copyResourcesToDirectory(URLConnection urlConnection,
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
        } catch (IOException e) {
          LOG.error("Error copying file", e);
        }
      }
    }
  }

  private static void extractJar(String sourceJar, String workDir) throws IOException {
    if (workDir == null) {
      workDir = System.getProperty("java.io.tmpdir");
    }
    final String folderToLoad = "";
    URL url = new URL("jar:file:" + sourceJar + "!/");
    final URLConnection urlConnection = (JarURLConnection) url.openConnection();
    File workDir_handler = new File(workDir + "/tmp");
    if (!workDir_handler.exists()) {
      workDir_handler.mkdirs();
    }

    if (urlConnection instanceof JarURLConnection) {
      final JarFile jarFile = ((JarURLConnection) urlConnection).getJarFile();
      extractResourcesToDirectory(jarFile, folderToLoad, workDir + "/tmp/");
    } else {
      throw new IOException(urlConnection.toString() + " is not JarUrlConnection");
    }
    /*
     * System.out.println("Current content under " + workDir + "/tmp/");
     * Files.list(new File(workDir + "/tmp/").toPath()).forEach(path -> {
     * System.out.println(path); });
     */
  }

  private static void extractResourcesToDirectory(JarFile origJar, String jarPath, String destPath) throws IOException {
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
          } catch (IOException e) {
            LOG.error("Error closing file", e);
          }
          try {
            outFile.close();
          } catch (IOException e) {
            LOG.error("Error closing file", e);
          }
        }
      }
    }
  }

}
