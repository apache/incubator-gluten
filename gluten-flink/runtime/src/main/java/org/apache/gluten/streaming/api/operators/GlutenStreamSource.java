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
package org.apache.gluten.streaming.api.operators;

import org.apache.gluten.table.runtime.operators.GlutenVectorSourceFunction;

import io.github.zhztheplayer.velox4j.connector.ConnectorSplit;
import io.github.zhztheplayer.velox4j.plan.StatefulPlanNode;
import io.github.zhztheplayer.velox4j.type.RowType;

import org.apache.flink.streaming.api.operators.StreamSource;

import java.util.Map;

/** Legacy stream source operator in gluten, which will call Velox to run. */
public class GlutenStreamSource extends StreamSource implements GlutenOperator {
  private final GlutenVectorSourceFunction sourceFunction;

  public GlutenStreamSource(GlutenVectorSourceFunction function) {
    super(function);
    sourceFunction = function;
  }

  @Override
  public void open() throws Exception {
    super.open();
  }

  @Override
  public StatefulPlanNode getPlanNode() {
    return sourceFunction.getPlanNode();
  }

  @Override
  public RowType getInputType() {
    return null;
  }

  @Override
  public Map<String, RowType> getOutputTypes() {
    return sourceFunction.getOutputTypes();
  }

  @Override
  public String getId() {
    return sourceFunction.getId();
  }

  public ConnectorSplit getConnectorSplit() {
    return sourceFunction.getConnectorSplit();
  }
}
