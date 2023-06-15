package io.glutenproject.integration.tpc.command;

import picocli.CommandLine;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class SparkRunModes {
  public interface Mode {
    String getSparkMasterUrl();
    Map<String, String> extraSparkConf();
  }

  public static class ModeEnumeration implements Mode {
    @CommandLine.ArgGroup(exclusive = false)
    LocalMode localMode;

    @CommandLine.ArgGroup(exclusive = false)
    LocalClusterMode localClusterMode;

    private Mode getActiveMode() {
      int enabledModeCount = 0;
      if (localMode != null) {
        enabledModeCount++;
      }
      if (localClusterMode != null) {
        enabledModeCount++;
      }

      if (enabledModeCount != 1) {
        throw new IllegalStateException("Only one single run mode can be specified");
      }

      if (localMode != null) {
        return localMode;
      }

      if (localClusterMode != null) {
        return localClusterMode;
      }

      throw new IllegalStateException("unreachable code");
    }

    @Override
    public String getSparkMasterUrl() {
      return getActiveMode().getSparkMasterUrl();
    }

    @Override
    public Map<String, String> extraSparkConf() {
      return getActiveMode().extraSparkConf();
    }
  }

  private static class LocalMode implements Mode {
    @CommandLine.Option(names = {"--local"}, description = "Run in Spark local mode", required = true)
    private boolean enabled;

    @CommandLine.Option(names = {"--local-threads"}, description = "Local mode: Run Spark locally with as many worker threads", defaultValue = "4")
    private int localThreads;

    @Override
    public String getSparkMasterUrl() {
      if (!enabled) {
        throw new IllegalStateException("Spark is not running in local mode");
      }

      return String.format("local[%d]", localThreads);
    }

    @Override
    public Map<String, String> extraSparkConf() {
      return Collections.emptyMap();
    }
  }

  private static class LocalClusterMode implements Mode {
    // We should transfer the jars to be tested in the integration testing to executors
    public static final String[] EXTRA_JARS = new String[]{"gluten-package-0.5.0-SNAPSHOT.jar"};

    @CommandLine.Option(names = {"--local-cluster"}, description = "Run in Spark local cluster mode", required = true)
    private boolean enabled;

    @CommandLine.Option(names = {"--local-cluster-workers"}, description = "Local cluster mode: Number of workers", defaultValue = "2")
    private int lcWorkers;

    @CommandLine.Option(names = {"--local-cluster-cores"}, description = "Local cluster mode: Number of cores per worker", defaultValue = "2")
    private int lcCores;

    @CommandLine.Option(names = {"--local-cluster-mem"}, description = "Local cluster mode: Memory per worker in MiB", defaultValue = "1024")
    private int lcMem;

    @Override
    public String getSparkMasterUrl() {
      if (!enabled) {
        throw new IllegalStateException("Spark is not running in local cluster mode");
      }
      if (!System.getenv().containsKey("SPARK_HOME")) {
        throw new IllegalArgumentException("SPARK_HOME not set! Please use --local if there is no local Spark build");
      }
      return String.format("local-cluster[%d,%d,%d]", lcWorkers, lcCores, lcMem);
    }

    @Override
    public Map<String, String> extraSparkConf() {
      final Set<String> extraJarSet = Arrays.stream(EXTRA_JARS).collect(Collectors.toSet());
      String classpath = System.getProperty("java.class.path");
      String[] classPathValues = classpath.split(File.pathSeparator);
      Optional<String> extraClassPath = Arrays.stream(classPathValues).filter(classPath -> {
        File file = new File(classPath);
        return file.exists() && file.isFile() && extraJarSet.contains(file.getName());
      }).reduce((s1, s2) -> s1 + File.pathSeparator + s2);

      final Map<String, String> extras = new HashMap<>();
      extraClassPath.ifPresent(path -> extras.put("spark.executor.extraClassPath", path));
      return extras;
    }
  }
}
