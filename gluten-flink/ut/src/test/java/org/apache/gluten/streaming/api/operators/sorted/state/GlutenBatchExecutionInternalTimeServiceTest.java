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
package org.apache.gluten.streaming.api.operators.sorted.state;

import org.apache.flink.streaming.api.operators.sorted.state.BatchExecutionInternalTimeServiceTest;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class GlutenBatchExecutionInternalTimeServiceTest
    extends BatchExecutionInternalTimeServiceTest {

  @Disabled
  @Test
  @Override
  public void testBatchExecutionManagerCanBeInstantiatedWithBatchStateBackend() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testForEachEventTimeTimerUnsupported() {}

  @Disabled
  @Test
  @Override
  public void testForEachProcessingTimeTimerUnsupported() {}

  @Disabled
  @Test
  @Override
  public void testFiringEventTimeTimers() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testSettingSameKeyDoesNotFireTimers() {}

  @Disabled
  @Test
  @Override
  public void testCurrentWatermark() {}

  @Disabled
  @Test
  @Override
  public void testProcessingTimeTimers() {}

  @Disabled
  @Test
  @Override
  public void testIgnoringEventTimeTimersFromWithinCallback() {}

  @Disabled
  @Test
  @Override
  public void testIgnoringProcessingTimeTimersFromWithinCallback() {}
}
