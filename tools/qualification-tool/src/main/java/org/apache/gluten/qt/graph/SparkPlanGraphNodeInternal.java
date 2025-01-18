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
package org.apache.gluten.qt.graph;

import java.util.List;

/** Internal representation of a {@code org.apache.spark.sql.execution.ui.SparkPlanGraphNode}. */
public class SparkPlanGraphNodeInternal {
  private final long id;
  private final String name;
  private final String desc;
  private final List<SqlPlanMetricInternal> metrics;

  public SparkPlanGraphNodeInternal(
      long id, String name, String desc, List<SqlPlanMetricInternal> metrics) {
    this.id = id;
    this.name = name;
    this.desc = desc;
    this.metrics = metrics;
  }

  public long getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getAnonymizedName() {
    if (name.contains("Scan")) {
      String[] nameParts = name.split(" ", -1);
      if (nameParts.length == 0) {
        return "";
      } else {
        return nameParts[0];
      }
    } else {
      return name;
    }
  }

  public String getDesc() {
    return desc;
  }

  public List<SqlPlanMetricInternal> getMetrics() {
    return metrics;
  }

  @Override
  public String toString() {
    return String.format("SparkPlanGraphNodeInternal{id=%d, name='%s', desc='%s'}", id, name, desc);
  }
}
