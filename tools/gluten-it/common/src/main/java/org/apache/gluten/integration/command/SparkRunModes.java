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
package org.apache.gluten.integration.command;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import picocli.CommandLine;

import javax.management.*;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public final class SparkRunModes {
  private static <T> T findNonNull(T... objects) {
    final List<T> nonNullObjects = Arrays.stream(objects).filter(Objects::nonNull).collect(Collectors.toList());
    Preconditions.checkState(nonNullObjects.size() == 1, "There are zero or more than one non-null objects: " + nonNullObjects);
    return nonNullObjects.get(0);
  }

  public interface Mode {
    String getSparkMasterUrl();

    Map<String, String> extraSparkConf();

    class Enumeration implements Mode {
      @CommandLine.ArgGroup(exclusive = false)
      LocalMode localMode;

      @CommandLine.ArgGroup(exclusive = false)
      LocalClusterMode localClusterMode;

      private Mode getActiveMode() {
        return findNonNull(localMode, localClusterMode);
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
  }

  private static class LocalMode implements Mode {
    @CommandLine.Option(names = {"--local"}, description = "Run in Spark local mode", required = true)
    private boolean enabled;

    @CommandLine.Option(names = {"--threads"}, description = "Local mode: Run Spark locally with as many worker threads", defaultValue = "4")
    private int localThreads;

    @CommandLine.Option(names = {"--off-heap-size"}, description = "Local mode: Total off-heap memory size", defaultValue = "6g")
    private String offHeapSize;

    @Override
    public String getSparkMasterUrl() {
      if (!enabled) {
        throw new IllegalStateException("Spark is not running in local mode");
      }

      return String.format("local[%d]", localThreads);
    }

    @Override
    public Map<String, String> extraSparkConf() {
      return ImmutableMap.<String, String>builder()
          .put("spark.memory.offHeap.enabled", "true")
          .put("spark.memory.offHeap.size", offHeapSize)
          .build();
    }
  }

  private interface ClusterResource {
    int lcWorkers();

    int lcWorkerCores();

    long lcWorkerHeapMem(); // in MiB.

    int lcExecutorCores();

    long lcExecutorHeapMem(); // in MiB.

    long lcExecutorOffHeapMem(); // in MiB.

    class Enumeration implements ClusterResource {
      @CommandLine.ArgGroup(exclusive = false)
      ManualClusterResource manual;

      @CommandLine.ArgGroup(exclusive = false)
      AutoClusterResource auto;

      private ClusterResource getActive() {
        return findNonNull(manual, auto);
      }

      @Override
      public int lcWorkers() {
        return getActive().lcWorkers();
      }

      @Override
      public int lcWorkerCores() {
        return getActive().lcWorkerCores();
      }

      @Override
      public long lcWorkerHeapMem() {
        return getActive().lcWorkerHeapMem();
      }

      @Override
      public int lcExecutorCores() {
        return getActive().lcExecutorCores();
      }

      @Override
      public long lcExecutorHeapMem() {
        return getActive().lcExecutorHeapMem();
      }

      @Override
      public long lcExecutorOffHeapMem() {
        return getActive().lcExecutorOffHeapMem();
      }
    }
  }

  private static class ManualClusterResource implements ClusterResource {
    @CommandLine.Option(names = {"--manual-cluster-resource"}, description = "Local cluster mode: Manually configure cluster resource", required = true)
    private boolean enabled;

    @CommandLine.Option(names = {"--workers"}, description = "Local cluster mode: Number of workers", defaultValue = "2")
    private int lcWorkers;

    @CommandLine.Option(names = {"--worker-cores"}, description = "Local cluster mode: Number of cores per worker", defaultValue = "2")
    private int lcWorkerCores;

    @CommandLine.Option(names = {"--worker-heap-size"}, description = "Local cluster mode: Heap memory per worker", defaultValue = "4g")
    private String lcWorkerHeapMem;

    @CommandLine.Option(names = {"--executor-cores"}, description = "Local cluster mode: Number of cores per executor", defaultValue = "1")
    private int lcExecutorCores;

    @CommandLine.Option(names = {"--executor-heap-size"}, description = "Local cluster mode: Heap memory per executor", defaultValue = "2g")
    private String lcExecutorHeapMem;

    @CommandLine.Option(names = {"--executor-off-heap-size"}, description = "Local cluster mode: Off-heap memory per executor", defaultValue = "6g")
    private String lcExecutorOffHeapMem;

    @Override
    public int lcWorkers() {
      ensureEnabled();
      return lcWorkers;
    }

    @Override
    public int lcWorkerCores() {
      ensureEnabled();
      return lcWorkerCores;
    }

    @Override
    public long lcWorkerHeapMem() {
      ensureEnabled();
      return Utils.byteStringAsMb(lcWorkerHeapMem);
    }

    @Override
    public int lcExecutorCores() {
      ensureEnabled();
      return lcExecutorCores;
    }

    @Override
    public long lcExecutorHeapMem() {
      ensureEnabled();
      return Utils.byteStringAsMb(lcExecutorHeapMem);
    }

    @Override
    public long lcExecutorOffHeapMem() {
      ensureEnabled();
      return Utils.byteStringAsMb(lcExecutorOffHeapMem);
    }

    private void ensureEnabled() {
      if (!enabled) {
        throw new IllegalStateException("Manual cluster resource is not enabled");
      }
    }
  }

  private static class AutoClusterResource implements ClusterResource {
    @CommandLine.Option(names = {"--auto-cluster-resource"}, description = "Local cluster mode: Automatically configure cluster resource", required = true)
    private boolean enabled;

    @CommandLine.Option(names = {"--off-heap-ratio"}, description = "Local cluster mode: Ratio assigned to executor off-heap memory out of total executor memory. The rest of total will be assigned to executor on-heap memory", defaultValue = "0.67")
    private double offHeapRatio;

    private int lcWorkers;
    private int lcWorkerCores;
    private long lcWorkerHeapMem;
    private int lcExecutorCores;
    private long lcExecutorHeapMem;
    private long lcExecutorOffHeapMem;

    private final AtomicBoolean initialized = new AtomicBoolean(false);

    public AutoClusterResource() {
    }

    private void ensureInitialized() {
      if (!initialized.compareAndSet(false, true)) {
        return;
      }
      Preconditions.checkArgument(offHeapRatio > 0 && offHeapRatio < 1, "Value of --off-heap-ratio should be in range (0, 1)");
      final int totalCores = Runtime.getRuntime().availableProcessors();
      final long totalMem = (long) (getTotalMem() * 0.8);
      Preconditions.checkState(totalMem >= 64, "--auto-cluster-resource mode requires for at least 64 MiB physical memory available. Current: " + totalMem);
      Preconditions.checkState(totalCores >= 1, "--auto-cluster-resource mode requires for at least 1 CPU core available. Current: " + totalCores);
      if (totalCores % 2 == 1) {
        // Platform has an odd number of CPU cores.
        this.lcWorkers = 1;
        this.lcWorkerCores = totalCores;
        this.lcExecutorCores = 1;
      } else {
        // Platform has an even number of CPU cores.
        this.lcWorkers = 2;
        this.lcWorkerCores = totalCores / this.lcWorkers;
        if (lcWorkerCores % 2 == 1) {
          this.lcExecutorCores = 1;
        } else {
          this.lcExecutorCores = 2;
        }
      }
      Preconditions.checkState(totalCores % this.lcExecutorCores == 0);
      final int numExecutors = totalCores / this.lcExecutorCores;
      Preconditions.checkState(this.lcWorkerCores % this.lcExecutorCores == 0);
      final int numExecutorsPerWorker = this.lcWorkerCores / this.lcExecutorCores;
      final long executorMem = totalMem / numExecutors;
      this.lcExecutorHeapMem = (long) (executorMem * (1 - offHeapRatio));
      this.lcExecutorOffHeapMem = (long) (executorMem * offHeapRatio);
      this.lcWorkerHeapMem = this.lcExecutorHeapMem * numExecutorsPerWorker;
      System.out.printf("Automatically configured cluster resource settings: %n" +
              "  lcWorkers: [%d]%n" +
              "  lcWorkerCores: [%d]%n" +
              "  lcWorkerHeapMem: [%dMiB]%n" +
              "  lcExecutorCores: [%d]%n" +
              "  lcExecutorHeapMem: [%dMiB]%n" +
              "  lcExecutorOffHeapMem: [%dMiB]%n",
          lcWorkers,
          lcWorkerCores,
          lcWorkerHeapMem,
          lcExecutorCores,
          lcExecutorHeapMem,
          lcExecutorOffHeapMem);
    }

    private static long getTotalMem() {
      try {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final Object attribute = mBeanServer.getAttribute(new ObjectName("java.lang", "type", "OperatingSystem"), "TotalPhysicalMemorySize");
        final long totalMem = Long.parseLong(attribute.toString()) / 1024 / 1024;
        return totalMem;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public int lcWorkers() {
      ensureInitialized();
      return lcWorkers;
    }

    @Override
    public int lcWorkerCores() {
      ensureInitialized();
      return lcWorkerCores;
    }

    @Override
    public long lcWorkerHeapMem() {
      ensureInitialized();
      return lcWorkerHeapMem;
    }

    @Override
    public int lcExecutorCores() {
      ensureInitialized();
      return lcExecutorCores;
    }

    @Override
    public long lcExecutorHeapMem() {
      ensureInitialized();
      return lcExecutorHeapMem;
    }

    @Override
    public long lcExecutorOffHeapMem() {
      ensureInitialized();
      return lcExecutorOffHeapMem;
    }
  }

  private static class LocalClusterMode implements Mode {
    @CommandLine.Option(names = {"--local-cluster"}, description = "Run in Spark local cluster mode", required = true)
    private boolean enabled;

    @CommandLine.ArgGroup(exclusive = true, multiplicity = "1")
    ClusterResource.Enumeration resourceEnumeration;

    @Override
    public String getSparkMasterUrl() {
      if (!enabled) {
        throw new IllegalStateException("Spark is not running in local cluster mode");
      }
      if (!System.getenv().containsKey("SPARK_HOME")) {
        throw new IllegalArgumentException("SPARK_HOME not set! Please use --local if there is no local Spark build");
      }
      if (!System.getenv().containsKey("SPARK_SCALA_VERSION")) {
        throw new IllegalArgumentException("SPARK_SCALA_VERSION not set! Please set it first or use --local instead. Example: export SPARK_SCALA_VERSION=2.12");
      }
      return String.format("local-cluster[%d,%d,%d]", resourceEnumeration.lcWorkers(), resourceEnumeration.lcWorkerCores(), resourceEnumeration.lcWorkerHeapMem());
    }

    @Override
    public Map<String, String> extraSparkConf() {
      final Map<String, String> extras = new HashMap<>();
      extras.put(SparkLauncher.EXECUTOR_DEFAULT_JAVA_OPTIONS, "-Dio.netty.tryReflectionSetAccessible=true");
      extras.put(SparkLauncher.EXECUTOR_CORES, String.valueOf(resourceEnumeration.lcExecutorCores()));
      extras.put(SparkLauncher.EXECUTOR_MEMORY, String.format("%dm", resourceEnumeration.lcExecutorHeapMem()));
      extras.put("spark.memory.offHeap.enabled", "true");
      extras.put("spark.memory.offHeap.size", String.format("%dm", resourceEnumeration.lcExecutorOffHeapMem()));
      return extras;
    }
  }
}
