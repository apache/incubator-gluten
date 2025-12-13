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
package org.apache.gluten.velox;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

public interface VeloxSourceSinkFactory {

  static final Logger LOG = LoggerFactory.getLogger(VeloxSourceSinkFactory.class);

  /** Match the conditions to determine if the operator can be offloaded to velox. */
  boolean match(Transformation<RowData> transformation);

  /** Build source transformation that offload the operator to velox. */
  Transformation<RowData> buildVeloxSource(
      Transformation<RowData> transformation, Map<String, Object> parameters);

  /** Build sink transformation that offload the operator to velox. */
  Transformation<RowData> buildVeloxSink(
      Transformation<RowData> transformation, Map<String, Object> parameters);

  /** Choose the matched source/sink factory by given transformation. */
  private static Optional<VeloxSourceSinkFactory> getFactory(
      Transformation<RowData> transformation) {
    ServiceLoader<VeloxSourceSinkFactory> factories =
        ServiceLoader.load(VeloxSourceSinkFactory.class);
    for (VeloxSourceSinkFactory factory : factories) {
      if (factory.match(transformation)) {
        return Optional.of(factory);
      }
    }
    return Optional.empty();
  }

  /** Build Velox source, or fallback to flink orignal source . */
  static Transformation<RowData> buildSource(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    Optional<VeloxSourceSinkFactory> factory = getFactory(transformation);
    if (factory.isEmpty()) {
      LOG.warn(
          "Not find matched factory to build velox source transformation, and we will use flink original transformation {} instead.",
          transformation.getClass().getName());
      return transformation;
    } else {
      return factory.get().buildVeloxSource(transformation, parameters);
    }
  }

  /** Build Velox sink, or fallback to flink original sink. */
  static Transformation<RowData> buildSink(
      Transformation<RowData> transformation, Map<String, Object> parameters) {
    Optional<VeloxSourceSinkFactory> factory = getFactory(transformation);
    if (factory.isEmpty()) {
      LOG.warn(
          "Not find matched factory to build velox sink transformation, and we will use flink original transformation {} instead.",
          transformation.getClass().getName());
      return transformation;
    } else {
      return factory.get().buildVeloxSink(transformation, parameters);
    }
  }
}
