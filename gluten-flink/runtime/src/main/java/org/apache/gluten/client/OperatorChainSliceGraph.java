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
package org.apache.gluten.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OperatorChainSliceGraph {
  private static final Logger LOG = LoggerFactory.getLogger(OperatorChainSliceGraph.class);
  private Map<Integer, OperatorChainSlice> slices;

  public OperatorChainSliceGraph() {
    slices = new HashMap<>();
  }

  public void addSlice(Integer id, OperatorChainSlice chainSlice) {
    slices.put(id, chainSlice);
  }

  public OperatorChainSlice getSlice(Integer id) {
    return slices.get(id);
  }

  public void removeSlice(Integer id) {
    slices.remove(id);
  }

  public OperatorChainSlice getSourceSlice() {
    List<OperatorChainSlice> sourceCandidates = new ArrayList<>();

    for (OperatorChainSlice chainSlice : slices.values()) {
      if (chainSlice.getInputs().isEmpty()) {
        sourceCandidates.add(chainSlice);
      }
    }

    if (sourceCandidates.isEmpty()) {
      throw new IllegalStateException(
          "No source suboperator chain found (no suboperator chain with empty inputs)");
    } else if (sourceCandidates.size() > 1) {
      throw new IllegalStateException(
          "Multiple source suboperator chains found: "
              + sourceCandidates.size()
              + " suboperator chains have empty inputs");
    }

    return sourceCandidates.get(0);
  }

  public Map<Integer, OperatorChainSlice> getSlices() {
    return slices;
  }

  public void dumpLog() {
    for (OperatorChainSlice chainSlice : slices.values()) {
      LOG.info("Slice ID: {}, offloadable: {}", chainSlice.id(), chainSlice.isOffloadable());
      LOG.info("  Inputs: {}", chainSlice.getInputs().toString());
      LOG.info("  Outputs: {}", chainSlice.getOutputs().toString());
      LOG.info(
          "  Operator Configs: {}",
          chainSlice.getOperatorConfigs().stream()
              .map(config -> config.getOperatorName() + "(" + config.getVertexID() + ")")
              .reduce((a, b) -> a + ", " + b)
              .orElse(""));
    }
  }
}
