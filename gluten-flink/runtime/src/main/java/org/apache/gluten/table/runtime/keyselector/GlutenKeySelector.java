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
package org.apache.gluten.table.runtime.keyselector;

import io.github.zhztheplayer.velox4j.stateful.StatefulRecord;

import org.apache.flink.api.java.functions.KeySelector;

/** A KeySelector which will extract key from RowVector. The key type is RowVector. */
public class GlutenKeySelector implements KeySelector<StatefulRecord, Integer> {

  private static final long serialVersionUID = -1L;

  public GlutenKeySelector() {}

  @Override
  public Integer getKey(StatefulRecord value) throws Exception {
    // TODO: calculate the key hash in velox, and get it here.
    return value.getKey();
  }
}
