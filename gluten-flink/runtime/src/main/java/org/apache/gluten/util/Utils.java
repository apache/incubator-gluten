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
package org.apache.gluten.util;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.tasks.StreamTaskException;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Map;

/** Utils to add and get some infos to StreamConfig. */
public class Utils {

  public static void setNodeToChainedOutputs(
      StreamConfig streamConfig, Map<String, Integer> nodeToChainedOutputs) {
    setConfigToStreamConfig("_nodeToChainedOutputs", streamConfig, nodeToChainedOutputs);
  }

  public static Map<String, Integer> getNodeToChainedOutputs(
      StreamConfig streamConfig, ClassLoader userClassLoader) {
    return getConfigFromStreamConfig("_nodeToChainedOutputs", streamConfig, userClassLoader);
  }

  public static void setNodeToNonChainedOutputs(
      StreamConfig streamConfig, Map<IntermediateDataSetID, String> nodeToChainedOutputs) {
    setConfigToStreamConfig("_nodeToNonChainedOutputs", streamConfig, nodeToChainedOutputs);
  }

  public static Map<IntermediateDataSetID, String> getNodeToNonChainedOutputs(
      StreamConfig streamConfig, ClassLoader userClassLoader) {
    return getConfigFromStreamConfig("_nodeToNonChainedOutputs", streamConfig, userClassLoader);
  }

  private static <T> T getConfigFromStreamConfig(
      String key, StreamConfig streamConfig, ClassLoader userClassLoader) {
    try {
      return InstantiationUtil.readObjectFromConfig(
          streamConfig.getConfiguration(), key, userClassLoader);
    } catch (Exception e) {
      throw new StreamTaskException("Could not instantiate serializer.", e);
    }
  }

  private static void setConfigToStreamConfig(
      String key, StreamConfig streamConfig, Object object) {
    try {
      InstantiationUtil.writeObjectToConfig(object, streamConfig.getConfiguration(), key);
    } catch (IOException e) {
      throw new StreamTaskException(
          String.format("Could not serialize object for key %s.", key), e);
    }
  }
}
