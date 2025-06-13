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
package org.apache.gluten.streaming.api.operators.async;

import org.apache.flink.streaming.api.operators.async.AsyncWaitOperatorTest;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class GlutenAsyncWaitOperatorTest extends AsyncWaitOperatorTest {

  @Disabled
  @Test
  @Override
  public void testEventTimeOrdered() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testWaterMarkUnordered() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testProcessingTimeOrdered() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testProcessingUnordered() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testOperatorChainWithProcessingTime() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testStateSnapshotAndRestore() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testObjectReused() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testAsyncTimeoutFailure() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testAsyncTimeoutIgnore() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testTimeoutCleanup() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testTimeoutAfterComplete() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testOrderedWaitUserExceptionHandling() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testOrderedWaitUserExceptionHandlingWithRetry() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testUnorderedWaitUserExceptionHandling() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testUnorderedWaitUserExceptionHandlingWithRetry() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testOrderedWaitTimeoutHandling() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testOrderedWaitTimeoutHandlingWithRetry() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testUnorderedWaitTimeoutHandling() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testUnorderedWaitTimeoutHandlingWithRetry() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testRestartWithFullQueue() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testIgnoreAsyncOperatorRecordsOnDrain() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testIgnoreAsyncOperatorRecordsOnDrainWithRetry() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testProcessingTimeOrderedWithRetry() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testProcessingTimeUnorderedWithRetry() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testProcessingTimeRepeatedCompleteUnorderedWithRetry() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testProcessingTimeRepeatedCompleteOrderedWithRetry() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testProcessingTimeWithTimeoutFunctionUnorderedWithRetry() throws Exception {}

  @Disabled
  @Test
  @Override
  public void testProcessingTimeWithTimeoutFunctionOrderedWithRetry() throws Exception {}
}
