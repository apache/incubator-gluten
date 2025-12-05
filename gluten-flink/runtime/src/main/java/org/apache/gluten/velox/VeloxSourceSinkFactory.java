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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;

public interface VeloxSourceSinkFactory {

  /** Match the conditions to determine whether the operator can be offloaded to velox. */
  boolean match(Transformation<RowData> transformation);

  /** Build source transformation that offload the operator to velox. */
  Transformation<RowData> buildSource(
      Transformation<RowData> transformation,
      ScanTableSource tableSource,
      boolean checkpointEnabled);

  /** Build sink transformation that offload the operator to velox. */
  Transformation<RowData> buildSink(ReadableConfig config, Transformation<RowData> transformation);

  /** Choose the matched source/sink factory by given transformation. */
  static VeloxSourceSinkFactory getFactory(Transformation<RowData> transformation) {
    ServiceLoader<VeloxSourceSinkFactory> factories =
        ServiceLoader.load(VeloxSourceSinkFactory.class);
    Set<String> factoryNames = new HashSet<>();
    for (VeloxSourceSinkFactory factory : factories) {
      factoryNames.add(factory.getClass().getName());
      if (factory.match(transformation)) {
        return factory;
      }
    }
    throw new FlinkRuntimeException(
        "Not find implemented factory to build velox transformation, available factories:"
            + factoryNames);
  }
}
