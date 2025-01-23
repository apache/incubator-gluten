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

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.Map;

import static org.apache.gluten.qt.support.NotSupportedCategory.CHILD_NOT_SUPPORTED;

/**
 * A {@link GraphVisitor} that verifies child node support for each node in the {@link
 * ExecutionDescription}. If any child of a node is {@link NotSupported}, this visitor marks that
 * node as {@code NotSupported} too, indicating that the current node’s functionality depends on an
 * unsupported child.
 */
public class ChildSupportVisitor extends GraphVisitor {
  private static final String NOT_SUPPORTED_DESCRIPTION = "CHILD NOT SUPPORTED";

  public ChildSupportVisitor(ExecutionDescription executionDescription) {
    super(executionDescription);
  }

  @Override
  protected void visitor(long nodeId) {
    if (!resultSupportMap.containsKey(nodeId)) {
      Map<Long, GlutenSupport> childrenSupportMap = new HashMap<>();
      for (long childId : executionDescription.getChildren(nodeId)) {
        childrenSupportMap.put(childId, resultSupportMap.get(childId));
      }

      ImmutableList<NotSupported> notSupportedChildrenId =
          childrenSupportMap.entrySet().stream()
              .filter(e -> e.getValue() instanceof NotSupported)
              .map(e -> (NotSupported) e.getValue())
              .collect(ImmutableList.toImmutableList());

      GlutenSupport currentNodeSupport =
          executionDescription.getNodeIdToGluttenSupportMap().get(nodeId);

      final GlutenSupport resultSupport;
      if (notSupportedChildrenId.isEmpty()) {
        resultSupport = currentNodeSupport;
      } else if (currentNodeSupport instanceof NotSupported) {
        NotSupported ns = (NotSupported) currentNodeSupport;
        ns.addReason(CHILD_NOT_SUPPORTED, NOT_SUPPORTED_DESCRIPTION);
        resultSupport = ns;
      } else {
        resultSupport = new NotSupported(CHILD_NOT_SUPPORTED, NOT_SUPPORTED_DESCRIPTION);
      }
      resultSupportMap.put(nodeId, resultSupport);
    }
  }
}
