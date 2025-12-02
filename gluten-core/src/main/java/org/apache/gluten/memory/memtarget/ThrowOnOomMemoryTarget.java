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
package org.apache.gluten.memory.memtarget;

import org.apache.gluten.config.GlutenCoreConfig;

import org.apache.spark.memory.SparkMemoryUtil;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.task.TaskResources;
import org.apache.spark.util.Utils;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public class ThrowOnOomMemoryTarget implements MemoryTarget {
  private static final List<String> PRINTED_NON_BYTES_CONFIGURATIONS =
      Arrays.asList(
          GlutenCoreConfig.SPARK_OFFHEAP_ENABLED_KEY(),
          GlutenCoreConfig.DYNAMIC_OFFHEAP_SIZING_ENABLED().key());

  private static final List<String> PRINTED_BYTES_CONFIGURATIONS =
      Arrays.asList(
          GlutenCoreConfig.COLUMNAR_OFFHEAP_SIZE_IN_BYTES().key(),
          GlutenCoreConfig.COLUMNAR_TASK_OFFHEAP_SIZE_IN_BYTES().key(),
          GlutenCoreConfig.COLUMNAR_CONSERVATIVE_TASK_OFFHEAP_SIZE_IN_BYTES().key());

  private final MemoryTarget target;

  public ThrowOnOomMemoryTarget(MemoryTarget target) {
    this.target = target;
  }

  @Override
  public long borrow(long size) {
    long granted = target.borrow(size);
    if (granted >= size) {
      return granted;
    }
    // OOM happens.
    // Note if the target is a Spark memory consumer, spilling should already be requested but
    // failed to reclaim enough memory.
    if (granted != 0L) {
      target.repay(granted);
    }
    // Log memory usage
    if (TaskResources.inSparkTask()) {
      TaskResources.getLocalTaskContext().taskMemoryManager().showMemoryUsage();
    }
    // Build error message, then throw.
    final StringBuilder errorBuilder = new StringBuilder();
    errorBuilder
        .append(
            String.format(
                "Not enough spark off-heap execution memory. Acquired: %s, granted: %s. "
                    + "Try tweaking config option spark.memory.offHeap.size to get larger "
                    + "space to run this application "
                    + "(if spark.gluten.memory.dynamic.offHeap.sizing.enabled "
                    + "is not enabled). %n",
                Utils.bytesToString(size), Utils.bytesToString(granted)))
        .append("Current config settings: ")
        .append(System.lineSeparator());
    for (String confKey : PRINTED_NON_BYTES_CONFIGURATIONS) {
      errorBuilder
          .append(String.format("\t%s=%s", confKey, getSqlConfStringOrNa(confKey, v -> v)))
          .append(System.lineSeparator());
    }
    for (String confKey : PRINTED_BYTES_CONFIGURATIONS) {
      errorBuilder
          .append(
              String.format(
                  "\t%s=%s",
                  confKey, getSqlConfStringOrNa(confKey, ThrowOnOomMemoryTarget::reformatBytes)))
          .append(System.lineSeparator());
    }
    // Dump all consumer usages to exception body
    errorBuilder.append(SparkMemoryUtil.dumpMemoryTargetStats(target));
    errorBuilder.append(System.lineSeparator());
    throw new OutOfMemoryException(errorBuilder.toString());
  }

  private static String getSqlConfStringOrNa(String confKey, Function<String, String> ifPresent) {
    final SQLConf sqlConf = SQLConf.get();
    if (!sqlConf.contains(confKey)) {
      return "N/A";
    }
    return ifPresent.apply(sqlConf.getConfString(confKey));
  }

  private static String reformatBytes(String in) {
    return Utils.bytesToString(Utils.byteStringAsBytes(in));
  }

  @Override
  public long repay(long size) {
    return target.repay(size);
  }

  @Override
  public long usedBytes() {
    return target.usedBytes();
  }

  @Override
  public <T> T accept(MemoryTargetVisitor<T> visitor) {
    return visitor.visit(this);
  }

  public static class OutOfMemoryException extends RuntimeException {
    public OutOfMemoryException(String message) {
      super(message);
    }
  }

  public MemoryTarget target() {
    return target;
  }
}
