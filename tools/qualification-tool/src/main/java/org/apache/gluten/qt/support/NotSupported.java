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

import org.jspecify.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Indicates that a particular node or operator is not supported by Gluten, along with categorized
 * reasons for the lack of support.
 *
 * <p>Each {@link NotSupportedCategory} in the internal map may have a corresponding reason
 * (truncated to a maximum length for clarity). Additional reasons can be added via {@link
 * #addReason(NotSupportedCategory, String)}.
 */
public class NotSupported extends GlutenSupport {
  private final Map<NotSupportedCategory, String> notSupportedReasonMap = new HashMap<>();

  public NotSupported(NotSupportedCategory category, String reason) {
    notSupportedReasonMap.put(category, getFirstNChars(reason, 200));
  }

  public void addReason(NotSupportedCategory category, String reason) {
    notSupportedReasonMap.put(category, getFirstNChars(reason, 200));
  }

  public boolean isNotSupported(NotSupportedCategory category) {
    return notSupportedReasonMap.containsKey(category);
  }

  public String getCategoryReason(NotSupportedCategory category) {
    return notSupportedReasonMap.get(category);
  }

  private static @Nullable String getFirstNChars(String input, int n) {
    if (input == null) {
      return null;
    }
    return input.length() > n ? input.substring(0, n) : input;
  }
}
