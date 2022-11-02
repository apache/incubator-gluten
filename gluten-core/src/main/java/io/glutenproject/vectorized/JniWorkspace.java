package io.glutenproject.vectorized;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class JniWorkspace {
  private static final Logger LOG =
      LoggerFactory.getLogger(JniWorkspace.class);
  private static final Map<String, JniWorkspace> INSTANCES = new ConcurrentHashMap<>();
  private static final JniWorkspace DEFAULT_INSTANCE = createDefault();

  private final String workDir;
  private final JniLibLoader jniLibLoader;
  private final JniResourceHelper jniResourceHelper;

  private JniWorkspace(String rootDir) {
    try {
      LOG.info("Creating JNI workspace in root directory {}", rootDir);
      Path root = Paths.get(rootDir);
      Path created = Files.createTempDirectory(root, "spark_columnar_plugin_");
      this.workDir = created.toAbsolutePath().toString();
      this.jniLibLoader = new JniLibLoader(workDir);
      this.jniResourceHelper = new JniResourceHelper(workDir);
      LOG.info("JNI workspace {} created in root directory {}", workDir, rootDir);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static JniWorkspace createDefault() {
    try {
      final String tempRoot = System.getProperty("java.io.tmpdir");
      return createOrGet(tempRoot);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static JniWorkspace getDefault() {
    return DEFAULT_INSTANCE;
  }

  public static JniWorkspace createOrGet(String rootDir) {
    return INSTANCES.computeIfAbsent(rootDir, JniWorkspace::new);

  }

  public String getWorkDir() {
    return workDir;
  }

  public JniLibLoader libLoader() {
    return jniLibLoader;
  }

  public JniResourceHelper resourceHelper() {
    return jniResourceHelper;
  }
}
