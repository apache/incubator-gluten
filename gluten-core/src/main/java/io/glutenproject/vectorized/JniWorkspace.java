package io.glutenproject.vectorized;

import io.glutenproject.GlutenConfig;
import org.apache.commons.io.FileUtils;
import org.apache.spark.util.GlutenShutdownManager;
import org.apache.spark.util.SparkDirectoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;
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
      Path created = Files.createTempDirectory(root, "gluten-");
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
      final String tempRoot = SparkDirectoryUtil.namespace("jni")
          .mkChildDirRandomly(UUID.randomUUID().toString())
          .getAbsolutePath();
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
