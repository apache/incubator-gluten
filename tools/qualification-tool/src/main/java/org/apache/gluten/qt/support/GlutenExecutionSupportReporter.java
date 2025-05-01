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
package org.apache.gluten.qt.support;

import java.time.Duration;
import java.util.Map;

/**
 * Collects and reports information about the parts of a SQL workload that are supported by the
 * Gluten engine.
 *
 * <p>Specifically, this class calculates the total SQL execution time, the portion of that time
 * that is supported by Gluten, and identifies any operators requiring implementation for full
 * support.
 */
public class GlutenExecutionSupportReporter {
  private final Duration supportedSqlTime;
  private final Duration totalSqlTime;
  private final Map<String, ResultVisitor.UnsupportedImpact> operatorToImplement;

  public GlutenExecutionSupportReporter(ExecutionDescription description) {
    ExecutionDescription description1 = new NodeSupportVisitor(description).visitAndTag();
    ExecutionDescription description2 = new ClusterSupportVisitor(description1).visitAndTag();
    ExecutionDescription description3 = new ChildSupportVisitor(description2).visitAndTag();

    ResultVisitor resultVisitor = new ResultVisitor(description3);
    resultVisitor.visit();

    this.supportedSqlTime = resultVisitor.getSupportedSqlTime();
    this.totalSqlTime = resultVisitor.getTotalSqlTime();
    this.operatorToImplement = resultVisitor.getUnsupportedOperatorImpactCostMap();
  }

  public Duration getSupportedSqlTime() {
    return supportedSqlTime;
  }

  public Map<String, ResultVisitor.UnsupportedImpact> getOperatorToImplement() {
    return operatorToImplement;
  }

  public Duration getTotalSqlTime() {
    return totalSqlTime;
  }
}
