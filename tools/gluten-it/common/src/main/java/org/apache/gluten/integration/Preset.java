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
package org.apache.gluten.integration;

import org.apache.gluten.integration.metrics.MetricMapper;
import org.apache.spark.SparkConf;

import java.util.HashMap;
import java.util.Map;

public final class Preset {
  private static final Map<String, Preset> presets = new HashMap<>();

  static {
    presets.put("vanilla", new Preset(Constants.VANILLA_CONF(), Constants.VANILLA_METRIC_MAPPER()));
    presets.put("velox", new Preset(Constants.VELOX_CONF(), Constants.VELOX_METRIC_MAPPER()));
    presets.put("velox-with-celeborn", new Preset(Constants.VELOX_WITH_CELEBORN_CONF(), Constants.VELOX_METRIC_MAPPER()));
    presets.put("velox-with-uniffle", new Preset(Constants.VELOX_WITH_UNIFFLE_CONF(), Constants.VELOX_METRIC_MAPPER()));
  }

  public static Preset get(String name) {
    if (!presets.containsKey(name)) {
      throw new IllegalArgumentException("Non-existing preset name: " + name);
    }
    return presets.get(name);
  }

  private final SparkConf conf;
  private final MetricMapper metricMapper;

  public Preset(SparkConf conf, MetricMapper metricMapper) {
    this.conf = conf;
    this.metricMapper = metricMapper;
  }

  public SparkConf getConf() {
    return conf;
  }

  public MetricMapper getMetricMapper() {
    return metricMapper;
  }
}
