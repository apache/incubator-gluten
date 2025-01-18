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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * An abstract class that defines a post-order traversal mechanism for a graph described by an
 * {@link ExecutionDescription}. Subclasses implement the {@link #visitor(long)} method to specify
 * how each node is processed.
 *
 * <p>During the traversal, this class keeps track of visited nodes to avoid redundant processing
 * and accumulates a support map, which is used to augment the final {@link ExecutionDescription}
 * instance.
 */
public abstract class GraphVisitor {
  protected final ExecutionDescription executionDescription;
  protected final Set<Long> visitedSet = new HashSet<>();
  protected final Map<Long, GlutenSupport> resultSupportMap = new HashMap<>();

  public GraphVisitor(ExecutionDescription executionDescription) {
    this.executionDescription = executionDescription;
  }

  public ExecutionDescription visitAndTag() {
    visit(executionDescription.getStartNodeId());
    return executionDescription.withNewSupportMap(resultSupportMap);
  }

  // Visit in post order
  protected void visit(long nodeId) {
    if (!visitedSet.contains(nodeId)) {
      for (long edge : executionDescription.getChildren(nodeId)) {
        visit(edge);
      }
      visitor(nodeId);
      visitedSet.add(nodeId);
    }
  }

  protected abstract void visitor(long nodeId);
}
