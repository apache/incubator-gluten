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
package org.apache.gluten.rexnode;

import java.util.List;

// In case more information is needed in the future, we can add more fields to this class.
public class RexConversionContext {
  private final List<String> inputAttributeNames;

  public RexConversionContext(List<String> inputAttributeNames) {
    this.inputAttributeNames = inputAttributeNames;
  }

  public List<String> getInputAttributeNames() {
    return inputAttributeNames;
  }
}
