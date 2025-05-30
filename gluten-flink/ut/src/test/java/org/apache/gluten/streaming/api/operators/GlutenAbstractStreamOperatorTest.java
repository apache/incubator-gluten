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

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorTest;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class GlutenAbstractStreamOperatorTest extends AbstractStreamOperatorTest {

  @Disabled
  @Test
  @Override
  public void testStateDoesNotInterfere() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testEventTimeTimersDontInterfere() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testProcessingTimeTimersDontInterfere() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testEnsureProcessingTimeTimerRegisteredOnRestore() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testProcessingTimeAndEventTimeDontInterfere() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testStateAndTimerStateShufflingScalingUp() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testStateAndTimerStateShufflingScalingDown() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testCustomRawKeyedStateSnapshotAndRestore() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testIdleWatermarkHandling() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testIdlenessForwarding() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testTwoInputsRecordAttributesForwarding() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testOneInputRecordAttributesForwarding() throws Exception {}
}
